package hydfs

import (
	"hash/fnv"
	"mp3-g02/utils"
	"sort"
	"sync"
)

// Ring manages the consistent hashing ring
type Ring struct {
	sync.RWMutex
	membership   *utils.MembershipList
	sortedHashes []uint32                // Sorted list of node hashes
	nodeMap      map[uint32]utils.NodeID // Map from hash to NodeID
}

// NewRing creates a new consistent hash ring
func NewRing(membership *utils.MembershipList) *Ring {
	r := &Ring{
		membership:   membership,
		sortedHashes: make([]uint32, 0),
		nodeMap:      make(map[uint32]utils.NodeID),
	}
	r.UpdateRing() // Initial population
	return r
}

// Hash is our hashing function for nodes and files
func (r *Ring) Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// UpdateRing rebuilds the ring based on the current membership list
// This should be called periodically and on join/fail events.
func (r *Ring) UpdateRing() {
	r.Lock()
	defer r.Unlock()

	members := r.membership.GetAllMembers() // Gets a snapshot
	r.sortedHashes = make([]uint32, 0, len(members))
	r.nodeMap = make(map[uint32]utils.NodeID)

	for _, member := range members {
		// IMPORTANT: Only add ALIVE members to the ring for routing
		if member.Status == utils.Alive {
			hash := r.Hash(member.ID.String())
			if _, exists := r.nodeMap[hash]; !exists {
				r.nodeMap[hash] = member.ID
				r.sortedHashes = append(r.sortedHashes, hash)
			}
		}
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool {
		return r.sortedHashes[i] < r.sortedHashes[j]
	})
}

// GetSuccessors finds N alive successors for a given filename
func (r *Ring) GetSuccessors(filename string, n int) []utils.NodeID {
	r.RLock()
	defer r.RUnlock()

	successors := make([]utils.NodeID, 0, n)
	if len(r.sortedHashes) == 0 {
		return successors
	}

	fileID := r.Hash(filename)

	// Find the first node hash >= fileID
	idx := sort.Search(len(r.sortedHashes), func(i int) bool {
		return r.sortedHashes[i] >= fileID
	})

	// Iterate (with wrap-around) until we find N *unique* nodes
	// Note: We already pre-filtered for ALIVE nodes in UpdateRing()
	uniqueNodes := make(map[string]bool)
	attempts := 0
	for len(successors) < n && attempts < len(r.sortedHashes) {
		if idx == len(r.sortedHashes) {
			idx = 0 // Wrap around
		}

		nodeHash := r.sortedHashes[idx]
		node := r.nodeMap[nodeHash]

		if !uniqueNodes[node.String()] {
			successors = append(successors, node)
			uniqueNodes[node.String()] = true
		}

		idx++
		attempts++
	}
	return successors
}

// GetNodeHashes returns a map of NodeID.String() to its hash for list_mem_ids
func (r *Ring) GetNodeHashes() map[string]uint32 {
	r.RLock()
	defer r.RUnlock()

	hashes := make(map[string]uint32)
	for hash, nodeID := range r.nodeMap {
		hashes[nodeID.String()] = hash
	}
	// Also add non-alive nodes for a complete list
	allMembers := r.membership.GetAllMembers()
	for _, m := range allMembers {
		if _, ok := hashes[m.ID.String()]; !ok {
			hashes[m.ID.String()] = r.Hash(m.ID.String())
		}
	}
	return hashes
}
