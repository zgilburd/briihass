package admin

import (
	"bytes"
	"net/http"
)

// render executes the named template into a bytes.Buffer first and
// writes the result only on success. This avoids the html/template
// footgun where a partial render after headers were flushed leaves
// the client with a corrupted page and the operator with no signal
// that anything went wrong.
//
// Use this from every admin handler instead of calling
// s.tmpl.ExecuteTemplate(w, ...) directly. On render failure we
// return 500 with a friendly message; on a successful render any
// write failure (client disconnected mid-stream, broken pipe) is
// logged but not surfaced to the operator, since there is no longer
// anyone to surface it to.
func (s *Server) render(w http.ResponseWriter, name string, data any) {
	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		s.opts.Logger.Error("admin template render", "template", name, "err", err)
		http.Error(w, "internal error rendering page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := w.Write(buf.Bytes()); err != nil {
		s.opts.Logger.Warn("admin write response", "template", name, "err", err)
	}
}
