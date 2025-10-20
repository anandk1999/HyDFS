package hydfs

import (
	"mp3-g02/utils"
	"net/http"
	"sync"
)

// Config holds configuration for the HyDFS server
type Config struct {
	StoragePath string // Path to store file blocks and metadata
	ControlPort int    // Port for internal file transfers
}

// BlockInfo stores metadata for a single append operation (a block)
type BlockInfo struct {
	BlockID   string `json:"block_id"`  // Unique ID, e.g., clientID_timestamp
	ClientID  string `json:"client_id"` // ID of the client who appended
	Timestamp int64  `json:"timestamp"` // Timestamp of the append
	Size      int64  `json:"size"`      // Size of the block
}

// Metadata is the per-file log, stored as _meta.json
// This log defines the file's contents and order
type Metadata struct {
	sync.RWMutex
	FileID string      `json:"file_id"` // Hash of the HyDFSfilename
	Blocks []BlockInfo `json:"blocks"`  // Ordered list of blocks
}

// Server is the main HyDFS service component
type Server struct {
	sync.RWMutex
	config     Config
	membership *utils.MembershipList
	ring       *Ring
	store      *Store
	selfNodeID utils.NodeID
	stopChan   chan struct{} // For background tasks

	// New fields for distributed communication
	Client      *http.Client // HTTP client for node-to-node communication
	ControlPort int          // The port number for the control server
}

// Internal (node-to-node) request payloads

// metaResponse is used to gather metadata from replicas
type metaResponse struct {
	Meta *Metadata
	Node utils.NodeID
	Err  error
}
