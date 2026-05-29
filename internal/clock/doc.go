// Package clock provides an injectable Clock interface so packages that do
// time arithmetic (notably internal/presence with EWMA decay) can be tested
// deterministically.
//
// Real time is the production implementation. Tests use a Fake driven by
// t.Cleanup-managed time. See ADR-0006 for why the presence model needs
// time injection.
package clock
