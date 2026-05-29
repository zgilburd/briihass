package store

import (
	"context"
	"errors"
	"fmt"

	"briihass/internal/config"
)

// SeedFromYAMLIfEmpty calls LoadAll. If the store has never been
// written (ErrEmpty), the supplied YAML bytes are parsed via
// config.ParseTunables and saved as the initial state. Any other
// error is returned unchanged.
//
// Use this on boot before passing the result to the presence engine.
func SeedFromYAMLIfEmpty(ctx context.Context, s Store, yamlSeed []byte) (*config.Tunables, error) {
	t, err := s.LoadAll(ctx)
	if err == nil {
		return t, nil
	}
	if !errors.Is(err, ErrEmpty) {
		return nil, fmt.Errorf("load existing tunables: %w", err)
	}
	seed, perr := config.ParseTunables(yamlSeed)
	if perr != nil {
		return nil, fmt.Errorf("parse seed yaml: %w", perr)
	}
	if serr := s.SaveAll(ctx, seed); serr != nil {
		return nil, fmt.Errorf("save seed: %w", serr)
	}
	return seed, nil
}
