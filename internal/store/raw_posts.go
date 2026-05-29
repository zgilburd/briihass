package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ContentEncoding identifies the wire encoding of a RawPost.Body.
// gzip is the production case (vRIoT always gzips); identity is
// allowed for test fixtures and local capture replay.
//
// EncodingIdentity is a non-empty named value so an unset field on
// a RawPost literal cannot silently masquerade as "identity" — the
// switch in InsertRawPost rejects the empty string explicitly.
type ContentEncoding string

const (
	EncodingGzip     ContentEncoding = "gzip"
	EncodingIdentity ContentEncoding = "identity"
)

// RawPost is one persisted ingest/heartbeat envelope. Body is the
// wire bytes; ContentEncoding tells the reader whether to gunzip
// before treating Body as JSON.
type RawPost struct {
	ID              int64
	ReceivedAt      time.Time
	Endpoint        string
	RemoteAddr      string
	ContentEncoding ContentEncoding
	Body            []byte
	BodySHA256      string
}

// InsertRawPost records the envelope and returns the assigned id.
// Endpoint is typically "/ingest" or "/heartbeat".
func (s *Postgres) InsertRawPost(ctx context.Context, p RawPost) (int64, error) {
	if p.Endpoint == "" {
		return 0, errors.New("InsertRawPost: endpoint required")
	}
	if len(p.Body) == 0 {
		return 0, errors.New("InsertRawPost: body required")
	}
	if p.BodySHA256 == "" {
		return 0, errors.New("InsertRawPost: body_sha256 required")
	}
	switch p.ContentEncoding {
	case EncodingGzip, EncodingIdentity:
	case "":
		return 0, errors.New("InsertRawPost: ContentEncoding required (use EncodingIdentity for plain)")
	default:
		return 0, fmt.Errorf("InsertRawPost: unknown ContentEncoding %q", p.ContentEncoding)
	}
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO raw_posts (received_at, endpoint, remote_addr, content_encoding, body_gzip, body_sha256)
		VALUES (COALESCE($1, now()), $2, NULLIF($3, ''), NULLIF($4, ''), $5, $6)
		RETURNING id
	`, nullTime(p.ReceivedAt), p.Endpoint, p.RemoteAddr, string(p.ContentEncoding), p.Body, p.BodySHA256).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert raw_post: %w", err)
	}
	return id, nil
}

// GetRawPost returns the envelope by id. Returns ErrNotFound when
// the row has been pruned by the retention worker.
func (s *Postgres) GetRawPost(ctx context.Context, id int64) (*RawPost, error) {
	var p RawPost
	var remote, ce *string
	err := s.pool.QueryRow(ctx, `
		SELECT id, received_at, endpoint, remote_addr, content_encoding, body_gzip, body_sha256
		  FROM raw_posts WHERE id = $1
	`, id).Scan(&p.ID, &p.ReceivedAt, &p.Endpoint, &remote, &ce, &p.Body, &p.BodySHA256)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get raw_post: %w", err)
	}
	if remote != nil {
		p.RemoteAddr = *remote
	}
	if ce != nil {
		// Legacy rows written before EncodingIdentity gained the
		// "identity" value carried "" for plain bodies. Normalize on
		// read so consumers always see one of the named constants.
		if *ce == "" {
			p.ContentEncoding = EncodingIdentity
		} else {
			p.ContentEncoding = ContentEncoding(*ce)
		}
	} else {
		p.ContentEncoding = EncodingIdentity
	}
	return &p, nil
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
