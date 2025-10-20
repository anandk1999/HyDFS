package utils

import (
	"math/rand"
	"sync"
	"time"
)

// MembershipList keeps track of who we currently believe is in the group.
type MembershipList struct {
	sync.RWMutex
	LocalNode     NodeID
	Members       map[string]*Member
	Incarnation   int32
	recentUpdates []MemberUpdate
	updateWindow  time.Duration
}

// NewMembershipList seeds the list with ourselves and sets up bookkeeping windows.
func NewMembershipList(localNode NodeID) *MembershipList {
	ml := &MembershipList{
		LocalNode:     localNode,
		Members:       make(map[string]*Member),
		Incarnation:   0,
		recentUpdates: []MemberUpdate{},
		updateWindow:  5 * time.Second,
	}

	ml.Members[localNode.String()] = &Member{
		ID:               localNode,
		HeartbeatCounter: 0,
		Incarnation:      0,
		Status:           Alive,
		LastHeartbeat:    time.Now(),
	}

	return ml
}

// AddMember stores or upgrades information about a remote node.
func (ml *MembershipList) AddMember(member *Member) {
	ml.Lock()
	defer ml.Unlock()

	existing, exists := ml.Members[member.ID.String()]
	if !exists || member.Incarnation > existing.Incarnation {
		ml.Members[member.ID.String()] = member
		ml.AddRecentUpdate(member)
	}
}

// UpdateMember bumps status/incarnation if the incoming info is fresh enough.
func (ml *MembershipList) UpdateMember(nodeID string, status MemberStatus, incarnation int32) {
	ml.Lock()
	defer ml.Unlock()

	if member, exists := ml.Members[nodeID]; exists {
		if incarnation >= member.Incarnation {
			oldStatus := member.Status
			member.Status = status
			member.Incarnation = incarnation

			if status == Alive {
				member.LastHeartbeat = time.Now()
			} else if status == Suspected && oldStatus == Alive {
				member.SuspicionStart = time.Now()
			}

			ml.AddRecentUpdate(member)
		}
	}
}

// RemoveMember marks the node as failed and forgets it.
func (ml *MembershipList) RemoveMember(nodeID string) {
	ml.Lock()
	defer ml.Unlock()

	if member, exists := ml.Members[nodeID]; exists {
		member.Status = Failed
		ml.AddRecentUpdate(member)
		delete(ml.Members, nodeID)
	}
}

// GetRandomMembers returns up to n alive members ignoring the exclude set.
func (ml *MembershipList) GetRandomMembers(n int, exclude []string) []*Member {
	ml.RLock()
	defer ml.RUnlock()

	var candidates []*Member
	excludeMap := make(map[string]bool)
	for _, e := range exclude {
		excludeMap[e] = true
	}

	for id, member := range ml.Members {
		if !excludeMap[id] && member.Status == Alive && id != ml.LocalNode.String() {
			candidates = append(candidates, member)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	if n > len(candidates) {
		n = len(candidates)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	return candidates[:n]
}

// GetRecentUpdates returns the latest piggyback data and trims out old entries.
func (ml *MembershipList) GetRecentUpdates(limit int) []MemberUpdate {
	ml.Lock()
	defer ml.Unlock()

	cutoff := time.Now().Add(-ml.updateWindow)
	var filtered []MemberUpdate
	for _, update := range ml.recentUpdates {
		if update.Timestamp.After(cutoff) {
			filtered = append(filtered, update)
		}
	}
	ml.recentUpdates = filtered

	if limit > len(filtered) {
		limit = len(filtered)
	}

	if len(filtered) == 0 {
		return nil
	}

	return filtered[:limit]
}

// GetLatestMemberUpdates walks the map and snapshots everyone's current view.
func (ml *MembershipList) GetLatestMemberUpdates() []MemberUpdate {
	ml.RLock()
	defer ml.RUnlock()

	if len(ml.Members) == 0 {
		return nil
	}

	now := time.Now()
	updates := make([]MemberUpdate, 0, len(ml.Members))
	for _, member := range ml.Members {
		timestamp := member.LastHeartbeat
		if timestamp.IsZero() {
			timestamp = now
		}
		updates = append(updates, MemberUpdate{
			NodeID:      member.ID,
			Heartbeat:   member.HeartbeatCounter,
			Incarnation: member.Incarnation,
			Status:      member.Status,
			Timestamp:   timestamp,
		})
	}
	return updates
}

// IncrementHeartbeat bumps our local heartbeat and queues an update.
func (ml *MembershipList) IncrementHeartbeat() int32 {
	ml.Lock()
	defer ml.Unlock()

	localKey := ml.LocalNode.String()
	member, exists := ml.Members[localKey]
	if !exists {
		return 0
	}

	member.HeartbeatCounter++
	member.LastHeartbeat = time.Now()
	ml.AddRecentUpdate(member)

	return member.HeartbeatCounter
}

// AddRecentUpdate adds a recent update. Caller must hold the lock.
// AddRecentUpdate serializes the member's state so others can catch up.
func (ml *MembershipList) AddRecentUpdate(member *Member) {
	update := MemberUpdate{
		NodeID:      member.ID,
		Heartbeat:   member.HeartbeatCounter,
		Incarnation: member.Incarnation,
		Status:      member.Status,
		Timestamp:   time.Now(),
	}
	ml.recentUpdates = append(ml.recentUpdates, update)
}

// AddRecentUpdateSafe is a thread-safe version of AddRecentUpdate
// AddRecentUpdateSafe wraps AddRecentUpdate with its own lock.
func (ml *MembershipList) AddRecentUpdateSafe(member *Member) {
	ml.Lock()
	defer ml.Unlock()
	ml.AddRecentUpdate(member)
}

// GetAllMembers returns shallow copies so callers can't mutate us.
func (ml *MembershipList) GetAllMembers() []*Member {
	ml.RLock()
	defer ml.RUnlock()

	members := make([]*Member, 0, len(ml.Members))
	for _, member := range ml.Members {
		members = append(members, &Member{
			ID:               member.ID,
			HeartbeatCounter: member.HeartbeatCounter,
			Incarnation:      member.Incarnation,
			Status:           member.Status,
			LastHeartbeat:    member.LastHeartbeat,
		})
	}
	return members
}

// GetSuspectedMembers filters the set down to the folks we currently doubt.
func (ml *MembershipList) GetSuspectedMembers() []*Member {
	ml.RLock()
	defer ml.RUnlock()

	var suspects []*Member
	for _, m := range ml.Members {
		if m.Status == Suspected {
			suspects = append(suspects, &Member{
				ID:               m.ID,
				HeartbeatCounter: m.HeartbeatCounter,
				Incarnation:      m.Incarnation,
				Status:           m.Status,
				LastHeartbeat:    m.LastHeartbeat,
				SuspicionStart:   m.SuspicionStart,
			})
		}
	}
	return suspects
}

// IncrementIncarnation is called when we need to supersede old info about us.
func (ml *MembershipList) IncrementIncarnation() {
	ml.Lock()
	defer ml.Unlock()
	ml.Incarnation++
}

// RefreshLocalNode updates the local node with a new timestamp (for rejoining)
// RefreshLocalNode swaps in a fresh timestamp so a rejoin looks like a new process.
func (ml *MembershipList) RefreshLocalNode() {
	ml.Lock()
	defer ml.Unlock()

	// Create a new NodeID with fresh timestamp
	newNodeID := NodeID{
		IP:        ml.LocalNode.IP,
		Port:      ml.LocalNode.Port,
		Timestamp: time.Now().Unix(),
	}

	// Remove old entry from members map
	delete(ml.Members, ml.LocalNode.String())

	// Update local node reference
	ml.LocalNode = newNodeID
	// Add the new local node to members with fresh info
	ml.Members[newNodeID.String()] = &Member{
		ID:               newNodeID,
		HeartbeatCounter: 0,
		Incarnation:      ml.Incarnation,
		Status:           Alive,
		LastHeartbeat:    time.Now(),
	}
}
