package config

// Resolved is the per-beacon tunable view consumed by the presence
// engine. It's the merge of the in-memory Tunables.Defaults block
// with the optional per-beacon Overrides entry, produced by
// (*Tunables).ResolveFor.
type Resolved struct {
	Alpha               float64
	GracePeriodS        int
	DecayRateDbPerS     float64
	PresenceFloorDbm    int
	TAwayMaxS           int
	StickyAfterArrivalS int
	HysteresisDb        float64
	ConfirmCount        int
}

// ResolveFor returns the effective tunable values for one beacon by
// name. If the beacon has no override entry (or has an empty one),
// every field is filled from Defaults. Otherwise the override fields
// that are non-nil replace the corresponding default.
//
// This method is safe to call from any goroutine because Tunables
// fields are immutable once Validate has returned — the admin UI
// produces a NEW *Tunables on each save and swaps it in via the
// presence engine's ApplyTunables entry point.
func (t *Tunables) ResolveFor(beaconName string) Resolved {
	r := Resolved{
		Alpha:               t.Defaults.Alpha,
		GracePeriodS:        t.Defaults.GracePeriodS,
		DecayRateDbPerS:     t.Defaults.DecayRateDbPerS,
		PresenceFloorDbm:    t.Defaults.PresenceFloorDbm,
		TAwayMaxS:           t.Defaults.TAwayMaxS,
		StickyAfterArrivalS: t.Defaults.StickyAfterArrivalS,
		HysteresisDb:        t.Defaults.HysteresisDb,
		ConfirmCount:        t.Defaults.ConfirmCount,
	}
	o, ok := t.Beacons[beaconName]
	if !ok {
		return r
	}
	if o.Alpha != nil {
		r.Alpha = *o.Alpha
	}
	if o.GracePeriodS != nil {
		r.GracePeriodS = *o.GracePeriodS
	}
	if o.DecayRateDbPerS != nil {
		r.DecayRateDbPerS = *o.DecayRateDbPerS
	}
	if o.PresenceFloorDbm != nil {
		r.PresenceFloorDbm = *o.PresenceFloorDbm
	}
	if o.TAwayMaxS != nil {
		r.TAwayMaxS = *o.TAwayMaxS
	}
	if o.StickyAfterArrivalS != nil {
		r.StickyAfterArrivalS = *o.StickyAfterArrivalS
	}
	if o.HysteresisDb != nil {
		r.HysteresisDb = *o.HysteresisDb
	}
	if o.ConfirmCount != nil {
		r.ConfirmCount = *o.ConfirmCount
	}
	return r
}
