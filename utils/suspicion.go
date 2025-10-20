package utils

import (
	"context"
	"sync"
	"time"
)

// Options bundles all the knobs the suspicion logic cares about.
type Options struct {
	SuspicionTimeout   time.Duration
	CheckInterval      time.Duration
	RequireReports     int
	ConfirmedRetention time.Duration

	OnSuspect func(target NodeID, inc int32, reporters []NodeID)
	OnConfirm func(target NodeID, inc int32)
	OnClear   func(target NodeID, inc int32)
}

// suspectEntry tracks reporters for a target/ incarnation combo.
type suspectEntry struct {
	target       NodeID
	incarnation  int32
	reporters    map[NodeID]time.Time
	firstReport  time.Time
	confirmed    bool
	confirmedAt  time.Time
	cleanupAfter time.Time
}

// SuspicionManager keeps tabs on suspects and decides when to escalate.
type SuspicionManager struct {
	mu         sync.Mutex
	membership *MembershipList
	network    *NetworkLayer
	Opts       Options
	entries    map[string]*suspectEntry
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	started    bool
}

// NewSuspicionManager fills in defaults and returns a ready manager.
func NewSuspicionManager(m *MembershipList, n *NetworkLayer, opts Options) *SuspicionManager {
	if opts.SuspicionTimeout == 0 {
		opts.SuspicionTimeout = 2 * time.Second
	}
	if opts.CheckInterval == 0 {
		opts.CheckInterval = 200 * time.Millisecond
	}
	if opts.RequireReports <= 0 {
		opts.RequireReports = 1
	}
	// default confirmed retention 30s if not set
	if opts.ConfirmedRetention == 0 {
		opts.ConfirmedRetention = 3 * time.Second
	}

	return &SuspicionManager{
		membership: m,
		network:    n,
		Opts:       opts,
		entries:    make(map[string]*suspectEntry),
	}
}

// Start spins up the background loop exactly once.
func (sm *SuspicionManager) Start(parent context.Context) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.started {
		return
	}
	sm.ctx, sm.cancel = context.WithCancel(parent)
	sm.wg.Add(1)
	go sm.loop()
	sm.started = true
}

// Stop cancels the loop and waits for it to wind down.
func (sm *SuspicionManager) Stop() {
	sm.mu.Lock()
	if !sm.started {
		sm.mu.Unlock()
		return
	}
	sm.cancel()
	sm.mu.Unlock()
	sm.wg.Wait()
	sm.mu.Lock()
	sm.started = false
	sm.mu.Unlock()
}

// ProcessSuspicion records a report and triggers callbacks when we have enough evidence.
func (sm *SuspicionManager) ProcessSuspicion(reporter NodeID, target NodeID, inc int32) {
	now := time.Now()
	key := target.String()

	if target.String() == sm.membership.LocalNode.String() {
		sm.handleSelfRefutation(inc)
		return
	}

	sm.mu.Lock()
	e, ok := sm.entries[key]
	if ok {
		if inc < e.incarnation {
			sm.mu.Unlock()
			return
		}
		if inc > e.incarnation {
			e = &suspectEntry{
				target:      target,
				incarnation: inc,
				reporters:   make(map[NodeID]time.Time),
			}
			sm.entries[key] = e
		}
	} else {
		sm.mu.Unlock()
		sm.membership.Lock()
		member := sm.membership.Members[target.String()]
		localInc := int32(0)
		if member != nil {
			localInc = member.Incarnation
		}
		sm.membership.Unlock()
		if inc < localInc {
			return
		}
		sm.mu.Lock()
		e = &suspectEntry{
			target:      target,
			incarnation: inc,
			reporters:   make(map[NodeID]time.Time),
		}
		sm.entries[key] = e
	}

	e.reporters[reporter] = now

	shouldTransitionToSuspected := len(e.reporters) >= sm.Opts.RequireReports && e.firstReport.IsZero()
	if shouldTransitionToSuspected {
		e.firstReport = now
	}

	// Get reporters list while still holding lock if needed for callback
	var reporters []NodeID
	if shouldTransitionToSuspected && sm.Opts.OnSuspect != nil {
		reporters = sm.reportersList(e)
	}
	sm.mu.Unlock()

	if shouldTransitionToSuspected {
		sm.membership.Lock()
		m := sm.membership.Members[target.String()]
		if m != nil {
			m.Status = Suspected
			m.SuspicionStart = now
		}
		sm.membership.Unlock()

		if sm.Opts.OnSuspect != nil {
			go sm.Opts.OnSuspect(target, inc, reporters)
		}
	}
}

// ClearSuspect drops suspicion state if we hear the target is alive again.
func (sm *SuspicionManager) ClearSuspect(target NodeID, inc int32) {
	key := target.String()

	sm.mu.Lock()
	e, ok := sm.entries[key]
	if !ok {
		sm.mu.Unlock()
		sm.membership.Lock()
		m := sm.membership.Members[key]
		if m != nil {
			if inc >= m.Incarnation {
				m.Status = Alive
				m.Incarnation = inc
			}
		}
		sm.membership.Unlock()
		return
	}
	if inc < e.incarnation {
		sm.mu.Unlock()
		return
	}
	delete(sm.entries, key)
	sm.mu.Unlock()

	sm.membership.Lock()
	if m := sm.membership.Members[key]; m != nil {
		if inc >= m.Incarnation {
			m.Status = Alive
			m.Incarnation = inc
		}
	}
	sm.membership.Unlock()

	if sm.Opts.OnClear != nil {
		go sm.Opts.OnClear(target, inc)
	}
}

// handleSelfRefutation bumps our incarnation and shouts alive when the cluster doubts us.
func (sm *SuspicionManager) handleSelfRefutation(incomingInc int32) {
	sm.membership.Lock()
	localInc := sm.membership.Incarnation
	newInc := localInc
	if incomingInc >= newInc {
		newInc = incomingInc + 1
	} else {
		newInc = localInc + 1
	}
	sm.membership.Incarnation = newInc
	sm.membership.Unlock()

	msg := Message{
		Type:        AliveMsg,
		Sender:      sm.membership.LocalNode,
		Incarnation: newInc,
	}

	sm.membership.Lock()
	recipients := make([]string, 0, len(sm.membership.Members))
	for _, m := range sm.membership.Members {
		if m.ID.String() == sm.membership.LocalNode.String() {
			continue
		}
		recipients = append(recipients, m.ID.Address())
	}
	sm.membership.Unlock()

	for _, addr := range recipients {
		sm.network.Send(msg, addr)
	}
}

// loop wakes up on a ticker and checks which suspects have expired.
func (sm *SuspicionManager) loop() {
	defer sm.wg.Done()

	t := time.NewTicker(sm.Opts.CheckInterval)
	defer t.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case now := <-t.C:
			sm.check(now)
		}
	}
}

// check decides whether to confirm or cleanup any outstanding entries.
func (sm *SuspicionManager) check(now time.Time) {
	type confirmAction struct {
		target NodeID
		inc    int32
	}

	var toConfirm []confirmAction
	var toCleanup []string

	sm.mu.Lock()
	for key, e := range sm.entries {
		if e.firstReport.IsZero() {
			continue
		}
		if e.confirmed {
			if sm.Opts.ConfirmedRetention > 0 && !e.cleanupAfter.IsZero() && now.After(e.cleanupAfter) {
				toCleanup = append(toCleanup, key)
			}
			continue
		}
		if now.Sub(e.firstReport) >= sm.Opts.SuspicionTimeout {
			e.confirmed = true
			e.confirmedAt = now
			if sm.Opts.ConfirmedRetention > 0 {
				e.cleanupAfter = now.Add(sm.Opts.ConfirmedRetention)
			}
			toConfirm = append(toConfirm, confirmAction{target: e.target, inc: e.incarnation})
		}
	}
	for _, k := range toCleanup {
		delete(sm.entries, k)
	}
	sm.mu.Unlock()

	// Apply all membership status changes atomically
	if len(toConfirm) > 0 {
		sm.membership.Lock()
		var confirmedMembers []confirmAction
		for _, c := range toConfirm {
			if m := sm.membership.Members[c.target.String()]; m != nil {
				m.Status = Failed
				confirmedMembers = append(confirmedMembers, c)
			}
		}
		sm.membership.Unlock()

		// Call callbacks outside the lock
		for _, c := range confirmedMembers {
			if sm.Opts.OnConfirm != nil {
				go sm.Opts.OnConfirm(c.target, c.inc)
			}
		}
	}
}

// reportersList flattens the reporter map for logging callbacks.
func (sm *SuspicionManager) reportersList(e *suspectEntry) []NodeID {
	out := make([]NodeID, 0, len(e.reporters))
	for r := range e.reporters {
		out = append(out, r)
	}
	return out
}

// GetTimeout exposes the suspicion timeout (handy for tests/debug).
func (sm *SuspicionManager) GetTimeout() time.Duration {
	return sm.Opts.SuspicionTimeout
}
