// captured is a one-shot diagnostic HTTP capture endpoint for grabbing real
// POST payloads from the Ruckus vRIoT iBeacon plugin (or anything else)
// without sending internal data to a third-party capture service.
//
// Usage:
//
//	go run ./cmd/captured                              # listen on :8080, save to ./captures/
//	go run ./cmd/captured -addr :9000 -dir /tmp/caps   # custom
//
// captured has no auth and no validation. It logs every request, saves the
// full headers + body to disk, and returns 200 OK. The whole point is to
// discover what the client sends — including whatever auth headers, query
// params, content-type, and payload shape the vRIoT iBeacon plugin emits
// — so we can design the real bridge's contract around verified behavior.
// Configure the plugin's auth fields however you want; captured will record
// what shows up, and you can read the JSON afterward to see what it sent.
//
// For each request, captured:
//   - Logs a one-line summary to stderr (timestamp, method, URL, content-type,
//     remote addr, body length).
//   - Writes the full request (method, URL, headers, query, body) as JSON to
//     <dir>/<timestamp>-<short-hash>.json. Body is stored as UTF-8 when the
//     content is plain text or JSON, base64 otherwise. Truncated at -maxbody.
//   - Returns 200 OK with an empty body so the client does not retry.
//
// captured is **not** the briihass bridge. It is a development aid. Do not
// deploy it to production. Do not point it at the internet.
package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

type captureRecord struct {
	Timestamp       time.Time           `json:"timestamp"`
	RemoteAddr      string              `json:"remote_addr"`
	Method          string              `json:"method"`
	URL             string              `json:"url"`
	Host            string              `json:"host"`
	ContentType     string              `json:"content_type"`
	ContentEncoding string              `json:"content_encoding,omitempty"`
	Headers         map[string][]string `json:"headers"`
	Query           map[string][]string `json:"query,omitempty"`
	BodyLen         int                 `json:"body_len"`                    // bytes on the wire
	BodyRawBase64   string              `json:"body_raw_base64"`             // always populated — never lossy
	BodyDecodedText string              `json:"body_decoded_text,omitempty"` // decompressed + UTF-8 valid
	BodyDecodedLen  int                 `json:"body_decoded_len,omitempty"`  // bytes after decompression
	DecodeNote      string              `json:"decode_note,omitempty"`       // e.g. "gzip-decompressed", "invalid utf-8"
	Truncated       bool                `json:"truncated,omitempty"`
}

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	dir := flag.String("dir", "./captures", "directory to write capture files into")
	maxBody := flag.Int64("maxbody", 1<<20, "max bytes of request body to capture (default 1 MiB)")
	flag.Parse()

	if err := os.MkdirAll(*dir, 0o700); err != nil {
		log.Fatalf("mkdir %s: %v", *dir, err)
	}

	srv := &http.Server{
		Addr:              *addr,
		ReadHeaderTimeout: 10 * time.Second,
		Handler:           &handler{dir: *dir, maxBody: *maxBody},
	}

	printBanner(*addr, *dir)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
}

type handler struct {
	dir     string
	maxBody int64
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, truncated, err := readBody(r.Body, h.maxBody)
	if err != nil {
		log.Printf("read body: %v", err)
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}

	rec := captureRecord{
		Timestamp:       time.Now().UTC(),
		RemoteAddr:      r.RemoteAddr,
		Method:          r.Method,
		URL:             r.URL.String(),
		Host:            r.Host,
		ContentType:     r.Header.Get("Content-Type"),
		ContentEncoding: r.Header.Get("Content-Encoding"),
		Headers:         r.Header,
		Query:           r.URL.Query(),
		BodyLen:         len(body),
		Truncated:       truncated,
		BodyRawBase64:   base64.StdEncoding.EncodeToString(body),
	}

	decoded, note := decodeBody(rec.ContentEncoding, body)
	if decoded != nil {
		rec.BodyDecodedLen = len(decoded)
		if utf8.Valid(decoded) {
			rec.BodyDecodedText = string(decoded)
		} else {
			note = strings.TrimSpace(note + " (non-utf8 after decode — see body_raw_base64)")
		}
	}
	rec.DecodeNote = note

	name, err := writeRecord(h.dir, &rec)
	if err != nil {
		log.Printf("write record: %v", err)
	}

	log.Printf("%s %s from %s ct=%q ce=%q len=%d decoded=%d -> %s",
		r.Method, r.URL.String(), r.RemoteAddr,
		rec.ContentType, rec.ContentEncoding, rec.BodyLen, rec.BodyDecodedLen, name)

	w.WriteHeader(http.StatusOK)
}

// decodeBody returns the decoded body bytes (e.g. gunzipped) and a short
// human-readable note. Returns nil bytes if no decoding was applied or if
// decoding failed; the raw bytes are always preserved in body_raw_base64.
func decodeBody(contentEncoding string, body []byte) ([]byte, string) {
	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "", "identity":
		if utf8.Valid(body) {
			return body, ""
		}
		return nil, "raw body is not valid utf-8 — see body_raw_base64"
	case "gzip", "x-gzip":
		gr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Sprintf("gzip decode failed: %v", err)
		}
		defer gr.Close()
		out, err := io.ReadAll(gr)
		if err != nil {
			return nil, fmt.Sprintf("gzip read failed: %v", err)
		}
		return out, "gzip-decompressed"
	default:
		return nil, fmt.Sprintf("unknown content-encoding %q — see body_raw_base64", contentEncoding)
	}
}

func readBody(rc io.ReadCloser, max int64) ([]byte, bool, error) {
	defer rc.Close()
	buf, err := io.ReadAll(io.LimitReader(rc, max+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(buf)) > max {
		return buf[:max], true, nil
	}
	return buf, false, nil
}

func writeRecord(dir string, rec *captureRecord) (string, error) {
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	short := hex.EncodeToString(sum[:4])
	ts := rec.Timestamp.Format("20060102T150405.000")
	name := filepath.Join(dir, fmt.Sprintf("%s-%s-%s.json",
		ts, sanitize(rec.Method), short))
	return name, os.WriteFile(name, data, 0o600)
}

func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		out = append(out, '_')
	}
	return string(out)
}

func printBanner(addr, dir string) {
	abs, _ := filepath.Abs(dir)
	fmt.Fprintf(os.Stderr, "captured listening on %s\n", addr)
	fmt.Fprintf(os.Stderr, "writing requests to %s\n", abs)
	fmt.Fprintf(os.Stderr, "no auth — every request is logged + saved + 200 OK\n")
	ips := lanIPs()
	if len(ips) == 0 {
		fmt.Fprintf(os.Stderr, "no non-loopback IPs found — configure the client to use this host's reachable address\n")
		return
	}
	port := strings.TrimPrefix(addr, ":")
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		port = addr[i+1:]
	}
	fmt.Fprintf(os.Stderr, "\nconfigure the vRIoT iBeacon plugin (or any client) to POST to one of:\n")
	for _, ip := range ips {
		fmt.Fprintf(os.Stderr, "  http://%s:%s/ingest\n", ip, port)
	}
	fmt.Fprintf(os.Stderr, "\n(the path doesn't matter — captured accepts any path. /ingest matches the bridge's eventual endpoint.)\n\n")
}

func lanIPs() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	var out []string
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		ip4 := ipnet.IP.To4()
		if ip4 == nil {
			continue
		}
		out = append(out, ip4.String())
	}
	return out
}
