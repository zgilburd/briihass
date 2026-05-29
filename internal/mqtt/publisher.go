// Package mqtt publishes briihass presence events to a Mosquitto
// broker using Home Assistant's MQTT Discovery convention. See
// ADR-0002 for the contract.
package mqtt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"briihass/internal/presence"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// OnMarshalFailed is the optional callback fired when a JSON marshal
// for an outbound payload fails. kind is "discovery" or "attributes".
// Wired to a labeled Prometheus counter in main; nil-safe.
type OnMarshalFailed func(kind string)

// Options configures the publisher.
type Options struct {
	// BrokerURL is the paho scheme://host:port, e.g.
	//   tcp://localhost:1883
	BrokerURL string

	// Username and Password authenticate against the broker.
	// allow_anonymous false on the cluster Mosquitto, so both required.
	Username string
	Password string

	// ClientID is the paho client identifier. Defaults to "briihass".
	ClientID string

	// DiscoveryPrefix is the topic root HA's MQTT integration scans.
	// Defaults to "homeassistant".
	DiscoveryPrefix string

	// HABirthTopic is the topic HA publishes its birth/will message to
	// ("online"/"offline"). On "online" the publisher re-asserts all
	// discovery configs (the recommended HA repave trigger). Defaults to
	// "homeassistant/status".
	HABirthTopic string

	// BufferSize bounds the in-memory queue when the broker is
	// unreachable. Oldest events drop on overflow. Default 5000
	// (~5 minutes at ~14 events/sec sustained; topology is now
	// operator-promoted so the AP count is variable — calibrate
	// against the actual sustained rate, not a fixed multiplier).
	BufferSize int

	// ConnectTimeout caps the initial connect attempt.
	// Reconnects use paho's exponential backoff.
	ConnectTimeout time.Duration

	// Logger receives info/warn/error logs. Required.
	Logger *slog.Logger

	// OnMarshalFailed is called on each outbound JSON-marshal failure.
	// Optional; nil-safe.
	OnMarshalFailed OnMarshalFailed
}

func (o *Options) applyDefaults() {
	if o.ClientID == "" {
		o.ClientID = "briihass"
	}
	if o.DiscoveryPrefix == "" {
		o.DiscoveryPrefix = "homeassistant"
	}
	if o.HABirthTopic == "" {
		o.HABirthTopic = "homeassistant/status"
	}
	if o.BufferSize <= 0 {
		o.BufferSize = 5000
	}
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = 10 * time.Second
	}
}

// Publisher owns a paho MQTT client and a bounded send queue. The
// pipeline produces PresenceEvents into Publish() and a single
// background goroutine in Run() drains them to the broker.
type Publisher struct {
	opts Options
	cli  paho.Client

	// queue carries (event, isFirstSeen) tuples. Tracked separately
	// from the engine so retained Discovery messages publish only on
	// first observation.
	queue chan queueItem

	// seen tracks entities we've already sent the device discovery config
	// for. Cleared on our reconnect AND on an HA birth ("online") message
	// so config is re-asserted. seenIDs is a parallel map keyed by the
	// same entityID so an orphan reconcile can reconstruct the BeaconKey
	// for a remove without re-parsing the slug. hasVoltage/hasTemp track
	// which optional sensor components have been declared per entity
	// (lazy growth — the config is re-published to add a component the
	// first time its telemetry appears). All four are cleared together.
	mu         sync.Mutex
	seen       map[string]struct{}
	seenIDs    map[string]presence.BeaconKey
	hasVoltage map[string]bool
	hasTemp    map[string]bool

	// onMarshalFailed counts MQTT JSON-marshal failures by kind
	// ("discovery"|"state"). Optional; nil-safe. Wired in main to
	// reg.MQTTMarshalFailed.WithLabelValues so a stuck marshal path is
	// distinguishable from broker-side publish failures on /metrics.
	onMarshalFailed func(kind string)

	// OnHAOnline fires when an HA birth "online" message arrives (and from
	// ResyncDiscovery). Wired in main to engine.RepublishAll so every
	// device's config + state is re-asserted. Nil-safe. Runs after the
	// seen/declared maps are cleared, so the re-emitted events re-publish
	// discovery config.
	OnHAOnline func()

	dropped atomic.Uint64
	pubOK   atomic.Uint64
	pubErr  atomic.Uint64

	// OnDropped fires whenever enqueue had to drop an item (either the
	// oldest under saturation, or the incoming item when even the drop-
	// and-retry couldn't make room). Nil-safe. The publisher does not log
	// directly — a wedged broker would swamp the log; callers wire this
	// to a rate-limited Warn in main.
	OnDropped func(info DropInfo)
}

// DropInfo is the operator-facing description of a single drop event
// passed to Publisher.OnDropped. Kind is "event" or "remove"; the
// payload fields are populated based on which.
type DropInfo struct {
	Kind   string // "event" or "remove"
	Beacon presence.BeaconKey
	// State and APMac populated when Kind == "event".
	State string
	APMac string
}

// queueItem carries either a normal presence event (the common case)
// or a remove-entity request (when an operator demotes a beacon). The
// kind discriminator keeps the queue single-typed so the existing
// drop-oldest backpressure applies uniformly.
type queueItem struct {
	kind   queueItemKind
	ev     presence.PresenceEvent // valid when kind == queueEvent
	remove presence.BeaconKey     // valid when kind == queueRemove
}

type queueItemKind uint8

const (
	queueEvent queueItemKind = iota
	queueRemove
)

// New constructs a Publisher with the broker not yet connected. Call
// Connect() before Run() (or let Run() handle connect via paho's
// AutoReconnect).
func New(opts Options) (*Publisher, error) {
	opts.applyDefaults()
	if opts.BrokerURL == "" {
		return nil, errors.New("mqtt.Options: BrokerURL required")
	}
	if opts.Username == "" || opts.Password == "" {
		return nil, errors.New("mqtt.Options: Username and Password required (cluster Mosquitto has allow_anonymous false)")
	}
	if opts.Logger == nil {
		return nil, errors.New("mqtt.Options: Logger required")
	}

	p := &Publisher{
		opts:            opts,
		queue:           make(chan queueItem, opts.BufferSize),
		seen:            make(map[string]struct{}),
		seenIDs:         make(map[string]presence.BeaconKey),
		hasVoltage:      make(map[string]bool),
		hasTemp:         make(map[string]bool),
		onMarshalFailed: opts.OnMarshalFailed,
	}

	co := paho.NewClientOptions().
		AddBroker(opts.BrokerURL).
		SetClientID(opts.ClientID).
		SetUsername(opts.Username).
		SetPassword(opts.Password).
		SetCleanSession(false).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(30*time.Second).
		SetConnectTimeout(opts.ConnectTimeout).
		SetOrderMatters(false).
		// Last-Will: if briihass drops, the broker publishes "offline" to
		// the bridge availability topic so HA marks every entity unavailable.
		SetBinaryWill(bridgeAvailabilityTopic, []byte(payloadOffline), 1, true).
		SetOnConnectHandler(func(c paho.Client) {
			opts.Logger.Info("mqtt connected", "broker", opts.BrokerURL)
			// Announce availability and force Discovery to be re-sent for
			// every known beacon (a fresh broker won't have our retained
			// config).
			c.Publish(bridgeAvailabilityTopic, 1, true, []byte(payloadOnline))
			p.clearSeen()
			// Subscribe to HA's birth message so an HA restart / broker
			// re-add / reload re-asserts discovery even while our own
			// connection stays up (the recommended HA repave trigger).
			tok := c.Subscribe(opts.HABirthTopic, 1, p.onBirthMessage)
			if tok.WaitTimeout(5*time.Second) && tok.Error() != nil {
				opts.Logger.Warn("mqtt subscribe HA birth topic failed",
					"topic", opts.HABirthTopic, "err", tok.Error())
			}
		}).
		SetConnectionLostHandler(func(c paho.Client, err error) {
			opts.Logger.Warn("mqtt connection lost", "err", err)
		})

	p.cli = paho.NewClient(co)
	return p, nil
}

// clearSeen resets all discovery-tracking maps so the next events
// re-publish device configs. Holds the mutex.
func (p *Publisher) clearSeen() {
	p.mu.Lock()
	p.seen = make(map[string]struct{})
	p.seenIDs = make(map[string]presence.BeaconKey)
	p.hasVoltage = make(map[string]bool)
	p.hasTemp = make(map[string]bool)
	p.mu.Unlock()
}

// onBirthMessage handles HA's birth/will publishes. On "online" it
// clears the seen/declared maps and triggers OnHAOnline (engine
// RepublishAll) so discovery is re-asserted. Runs on paho's callback
// goroutine, so it only clears state + triggers — never blocks.
func (p *Publisher) onBirthMessage(_ paho.Client, m paho.Message) {
	if strings.TrimSpace(string(m.Payload())) != payloadOnline {
		return
	}
	p.opts.Logger.Info("HA birth message received; re-asserting discovery",
		"topic", m.Topic())
	p.clearSeen()
	if p.OnHAOnline != nil {
		p.OnHAOnline()
	}
}

// ResyncDiscovery re-asserts all discovery config + state on demand
// (the admin "Resync HA" button). Clears the seen/declared maps and
// triggers OnHAOnline. Use after an HA-side change that didn't fire a
// birth message (e.g. a single entity manually deleted).
func (p *Publisher) ResyncDiscovery() {
	p.clearSeen()
	if p.OnHAOnline != nil {
		p.OnHAOnline()
	}
}

// Connect synchronously connects to the broker, returning the first
// connect error (or nil). Reconnects after this point are handled
// automatically by paho.
//
// Honors WaitTimeout's bool return — paho's token may have neither
// completed nor errored when WaitTimeout returns false (the broker
// is unreachable), so blindly returning tok.Error() in that case
// would mask the timeout as success and bypass the cold-start
// MQTTInitialConnectFailed counter.
func (p *Publisher) Connect() error {
	tok := p.cli.Connect()
	if !tok.WaitTimeout(p.opts.ConnectTimeout) {
		return fmt.Errorf("mqtt connect: timed out after %s", p.opts.ConnectTimeout)
	}
	return tok.Error()
}

// Publish enqueues an event for delivery. Non-blocking: if the queue
// is full, the OLDEST item is dropped to make room for the new one.
// This matches the design in ADR-0002's failure-modes table: better
// to lose the stalest event than wedge the ingest goroutine.
func (p *Publisher) Publish(ev presence.PresenceEvent) {
	p.enqueue(queueItem{kind: queueEvent, ev: ev})
}

// enqueue is the shared backpressure path used by both Publish (events)
// and RemoveEntity (remove requests). Returns true when the item made
// it onto the queue, false when it was dropped under saturation.
func (p *Publisher) enqueue(item queueItem) bool {
	select {
	case p.queue <- item:
		return true
	default:
		var dropped queueItem
		droppedOldest := false
		select {
		case dropped = <-p.queue:
			p.dropped.Add(1)
			droppedOldest = true
		default:
		}
		select {
		case p.queue <- item:
			if droppedOldest && p.OnDropped != nil {
				p.OnDropped(dropInfoFrom(dropped))
			}
			return true
		default:
			p.dropped.Add(1)
			if p.OnDropped != nil {
				p.OnDropped(dropInfoFrom(item))
			}
			return false
		}
	}
}

func dropInfoFrom(item queueItem) DropInfo {
	switch item.kind {
	case queueRemove:
		return DropInfo{Kind: "remove", Beacon: item.remove}
	default:
		return DropInfo{
			Kind:   "event",
			Beacon: item.ev.Beacon,
			State:  item.ev.State.String(),
			APMac:  item.ev.APMac,
		}
	}
}

// Run drains the queue and publishes to MQTT until ctx is cancelled.
// Run blocks; call from a goroutine.
func (p *Publisher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-p.queue:
			switch item.kind {
			case queueRemove:
				p.handleRemove(item.remove)
			default:
				p.handle(item.ev)
			}
		}
	}
}

// Close disconnects the broker client (graceful). Run should be
// allowed to exit (via its context) before calling Close.
func (p *Publisher) Close() error {
	if p.cli != nil && p.cli.IsConnected() {
		p.cli.Disconnect(250) // ms
	}
	return nil
}

// RemoveEntity wipes a previously-published device. Enqueues a
// queueRemove item; handleRemove on the drain goroutine publishes empty
// retained payloads to the device config + state topics (and the legacy
// 004b per-component topics) so HA removes the device and no stale
// retained payload leaks to the next subscribe.
//
// Returns ErrQueueFull if the publisher queue is saturated and the
// request was dropped. The caller (admin demote handler) should surface
// this so the operator can retry — the DB row is already deleted but
// the HA entity will linger until a successful remove publishes.
func (p *Publisher) RemoveEntity(ctx context.Context, beacon presence.BeaconKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.enqueue(queueItem{kind: queueRemove, remove: beacon}) {
		return ErrQueueFull
	}
	return nil
}

// handleRemove processes a queueRemove item: empty retained payloads on
// the device config + state topics (HA removes the whole device) plus
// the legacy 004b per-component topics so old retained configs don't
// linger as ghost entities after the device-based-discovery switch.
func (p *Publisher) handleRemove(beacon presence.BeaconKey) {
	entityID := EntityID(beacon)
	p.mu.Lock()
	delete(p.seen, entityID)
	delete(p.seenIDs, entityID)
	delete(p.hasVoltage, entityID)
	delete(p.hasTemp, entityID)
	p.mu.Unlock()
	p.publishOne(deviceConfigTopic(p.opts.DiscoveryPrefix, beacon), []byte{}, true)
	p.publishOne(stateTopic(beacon), []byte{}, true)
	p.publishOne(telemetryTopic(beacon), []byte{}, true)
	for _, t := range legacyTopics(p.opts.DiscoveryPrefix, beacon) {
		p.publishOne(t, []byte{}, true)
	}
}

// Stats returns operational counters useful for /metrics + /admin.
func (p *Publisher) Stats() Stats {
	return Stats{
		QueueDepth:    len(p.queue),
		QueueCapacity: cap(p.queue),
		Dropped:       p.dropped.Load(),
		PublishOK:     p.pubOK.Load(),
		PublishErr:    p.pubErr.Load(),
		Connected:     p.cli != nil && p.cli.IsConnected(),
	}
}

// Stats is the operator-visible health snapshot of the publisher.
type Stats struct {
	QueueDepth    int
	QueueCapacity int
	Dropped       uint64
	PublishOK     uint64
	PublishErr    uint64
	Connected     bool
}

// handle publishes one event: the device-based discovery config (on
// first sight, and re-published to grow voltage/temperature components
// the first time that telemetry appears) followed by the single JSON
// state document that drives the tracker + all sensors. Errors are
// logged + counted; we never block the engine on broker latency.
//
// seen/declared entries are set ONLY after a successful config marshal +
// publish attempt, so a transient marshal hiccup retries on the next
// event rather than leaving HA without a config forever.
func (p *Publisher) handle(ev presence.PresenceEvent) {
	entityID := EntityID(ev.Beacon)

	p.mu.Lock()
	_, alreadySeen := p.seen[entityID]
	hadVoltage := p.hasVoltage[entityID]
	hadTemp := p.hasTemp[entityID]
	p.mu.Unlock()

	// Grow the component set lazily: declare voltage/temperature the first
	// time their telemetry appears so non-TLM beacons get no empty sensors.
	withVoltage := hadVoltage || ev.BatteryMV != nil
	withTemp := hadTemp || ev.TemperatureC != nil
	needConfig := !alreadySeen || withVoltage != hadVoltage || withTemp != hadTemp

	if needConfig {
		body, err := marshalCompact(BuildDeviceDiscovery(p.opts.DiscoveryPrefix, ev, withVoltage, withTemp))
		if err != nil {
			p.opts.Logger.Error("mqtt marshal discovery", "err", err, "beacon", entityID)
			if p.onMarshalFailed != nil {
				p.onMarshalFailed("discovery")
			}
			return
		}
		p.publishOne(deviceConfigTopic(p.opts.DiscoveryPrefix, ev.Beacon), body, true)
		if !alreadySeen {
			// First config publish for this entity (this connection): clear
			// the legacy 004b per-component retained config, which shares
			// this device_tracker's unique_id and would otherwise collide
			// with (and mask) the device-based config in HA.
			for _, t := range legacyTopics(p.opts.DiscoveryPrefix, ev.Beacon) {
				p.publishOne(t, []byte{}, true)
			}
		}
		p.mu.Lock()
		p.seen[entityID] = struct{}{}
		p.seenIDs[entityID] = ev.Beacon
		p.hasVoltage[entityID] = withVoltage
		p.hasTemp[entityID] = withTemp
		p.mu.Unlock()
	}

	// The device_tracker reads a BARE state string (HA ignores
	// value_template for device_tracker under device-based discovery), and
	// HA only treats "home" as home — so the tracker carries home/not_home,
	// never the area label. The area label, sensors, and the tracker's
	// attributes read the JSON telemetry topic.
	p.publishOne(stateTopic(ev.Beacon), []byte(trackerState(ev.State)), true)

	body, err := marshalCompact(BuildState(ev))
	if err != nil {
		p.opts.Logger.Error("mqtt marshal telemetry", "err", err, "beacon", entityID)
		if p.onMarshalFailed != nil {
			p.onMarshalFailed("state")
		}
		return
	}
	p.publishOne(telemetryTopic(ev.Beacon), body, true)
}

func (p *Publisher) publishOne(topic string, payload []byte, retained bool) {
	tok := p.cli.Publish(topic, 0, retained, payload)
	if !tok.WaitTimeout(5 * time.Second) {
		p.pubErr.Add(1)
		p.opts.Logger.Warn("mqtt publish timed out", "topic", topic)
		return
	}
	if err := tok.Error(); err != nil {
		p.pubErr.Add(1)
		p.opts.Logger.Warn("mqtt publish error", "topic", topic, "err", err)
		return
	}
	p.pubOK.Add(1)
}

// ErrQueueFull is returned by RemoveEntity when both the initial
// enqueue and the drop-oldest retry failed. The caller should treat
// this as a transient failure and retry; the broker connection may be
// stuck but recoverable.
var ErrQueueFull = fmt.Errorf("mqtt: publisher queue saturated")

// RepublishOrphans diffs the publisher's in-memory "seen" set against
// the caller-supplied authoritative known list and enqueues a remove
// for every entity that isn't in known. Used by the admin retry path:
// after a demote whose MQTT step failed (entity lingers in HA), the
// next refresh-engine click can self-heal by reconciling the actual
// set of published entities against the current allowlist.
//
// Returns the number of removes enqueued, plus the first ErrQueueFull
// encountered. The publisher stays consistent even if a partial
// reconcile is hit by saturation — the next call to RepublishOrphans
// will retry the remaining orphans.
//
// Stateless by design: the publisher does not track "I tried to
// remove entity X" between restarts. Re-running after a process
// bounce is correct because OnConnect clears seen (Discovery is
// re-asserted on the next event); a beacon demoted while the bridge
// was down will have its empty-payload remove sent on the next refresh.
func (p *Publisher) RepublishOrphans(ctx context.Context, known []presence.BeaconKey) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	keep := make(map[string]struct{}, len(known))
	for _, b := range known {
		keep[EntityID(b)] = struct{}{}
	}
	p.mu.Lock()
	orphans := make([]presence.BeaconKey, 0)
	for entityID, beaconID := range p.seenIDs {
		if _, ok := keep[entityID]; ok {
			continue
		}
		orphans = append(orphans, beaconID)
	}
	p.mu.Unlock()
	var firstErr error
	enq := 0
	for _, b := range orphans {
		if !p.enqueue(queueItem{kind: queueRemove, remove: b}) {
			if firstErr == nil {
				firstErr = ErrQueueFull
			}
			continue
		}
		enq++
	}
	return enq, firstErr
}
