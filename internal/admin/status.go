package admin

import (
	"net/http"
	"sort"
	"time"
)

type statusPage struct {
	Now       time.Time
	Commit    string
	Beacons   []statusBeacon
	Heartbeat statusHeartbeat
	MQTT      statusMQTT
}

type statusBeacon struct {
	Name         string
	Kind         string
	Key          string
	CurrentZone  string
	CurrentAP    string
	LastArrival  time.Time
	StickyActive bool
	APs          []statusAP
}

type statusAP struct {
	Mac           string
	Name          string
	LastRSSI      int
	EWMARSSI      float64
	EffectiveRSSI float64
	AgeSeconds    float64
	InPresence    bool
}

type statusHeartbeat struct {
	Known   bool
	Online  []string
	Offline []string
}

type statusMQTT struct {
	Known         bool
	Connected     bool
	QueueDepth    int
	QueueCapacity int
	Dropped       uint64
	PublishOK     uint64
	PublishErr    uint64
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	snap := s.opts.Engine.Snapshot()

	page := statusPage{
		Now:    snap.AsOf,
		Commit: s.opts.BuildCommit,
	}
	for _, b := range snap.Beacons {
		zone := b.CurrentZone
		if zone == "" {
			zone = "not_home"
		}
		entry := statusBeacon{
			Name:         b.Name,
			Kind:         string(b.Beacon.Kind()),
			Key:          b.Beacon.Key(),
			CurrentZone:  zone,
			CurrentAP:    b.CurrentAP,
			LastArrival:  b.LastArrival,
			StickyActive: b.StickyActive,
		}
		for _, ap := range b.APs {
			age := 0.0
			if !ap.LastSightingTs.IsZero() {
				age = snap.AsOf.Sub(ap.LastSightingTs).Seconds()
			}
			entry.APs = append(entry.APs, statusAP{
				Mac:           ap.Mac,
				Name:          ap.Name,
				LastRSSI:      ap.LastRSSI,
				EWMARSSI:      ap.EWMARSSI,
				EffectiveRSSI: ap.EffectiveRSSI,
				AgeSeconds:    age,
				InPresence:    ap.InPresence,
			})
		}
		// Sort APs by EffectiveRSSI descending (closest first) for the table.
		sort.SliceStable(entry.APs, func(i, j int) bool {
			return entry.APs[i].EffectiveRSSI > entry.APs[j].EffectiveRSSI
		})
		page.Beacons = append(page.Beacons, entry)
	}
	// Stable order: beacon name asc.
	sort.SliceStable(page.Beacons, func(i, j int) bool {
		return page.Beacons[i].Name < page.Beacons[j].Name
	})

	if s.opts.Heartbeat != nil {
		on, off := s.opts.Heartbeat()
		page.Heartbeat = statusHeartbeat{Known: true, Online: on, Offline: off}
	}
	if s.opts.MQTT != nil {
		st := s.opts.MQTT()
		page.MQTT = statusMQTT{
			Known: true, Connected: st.Connected,
			QueueDepth: st.QueueDepth, QueueCapacity: st.QueueCapacity,
			Dropped: st.Dropped, PublishOK: st.PublishOK, PublishErr: st.PublishErr,
		}
	}

	s.render(w, "status.html", page)
}
