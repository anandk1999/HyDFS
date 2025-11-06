package hydfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"mp3-g02/utils"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

// NewServer creates the HyDFS server component
func NewServer(cfg Config, membership *utils.MembershipList, selfNodeID utils.NodeID, controlPort int) (*Server, error) {
	store, err := NewStore(cfg.StoragePath)
	if err != nil {
		return nil, err
	}

	s := &Server{
		config:     cfg,
		membership: membership,
		ring:       NewRing(membership),
		store:      store,
		selfNodeID: selfNodeID,
		stopChan:   make(chan struct{}),
		// Initialize new fields
		Client:      &http.Client{Timeout: 10 * time.Second},
		ControlPort: controlPort,
	}
	return s, nil
}

// Start launches background maintenance tasks
func (s *Server) Start() {
	log.Println("HyDFS Server started...")
	go s.backgroundTasks()
}

// Stop terminates background tasks
func (s *Server) Stop() {
	close(s.stopChan)
	log.Println("HyDFS Server stopped.")
}

// backgroundTasks periodically updates the ring and checks for re-replication
func (s *Server) backgroundTasks() {
	ticker := time.NewTicker(15 * time.Second) // Conservative: check every 15 seconds
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.ring.UpdateRing()

			// Detect membership changes (joins)
			currentCount := len(s.membership.GetAllMembers())
			if currentCount > s.lastMemberCount {
				log.Printf("[HyDFS] Membership increased from %d to %d nodes, triggering rebalancing",
					s.lastMemberCount, currentCount)
				// DISABLED: Don't trigger immediate re-replication on joins
				// Let the periodic background check handle it naturally
				// This prevents flooding when nodes rejoin during recovery
				// go s.TriggerReReplication("node join detected")
			}
			s.lastMemberCount = currentCount

			// Check for files that need re-replication
			s.checkForReReplication()

			// Periodic background merge to reconcile divergent file versions
			s.checkAndMergeDivergentFiles()
		}
	}
}

// checkAndMergeDivergentFiles periodically checks for files with divergent versions
// across replicas and automatically merges them to ensure consistency
func (s *Server) checkAndMergeDivergentFiles() {
	fileIDs, err := s.store.ListLocalFiles()
	if err != nil {
		return
	}

	for _, fileID := range fileIDs {
		// Get metadata from local storage to find filename
		localMeta, err := s.store.ReadMetadata(fileID)
		if err != nil || localMeta == nil {
			continue
		}

		filename := s.resolveFilename(localMeta)
		replicas := s.ring.GetSuccessors(filename, 3)
		if len(replicas) < 2 {
			continue // Need at least 2 replicas to detect divergence
		}

		// Get metadata from all replicas
		allMetas := s.performGetMetadata(replicas, fileID)
		if len(allMetas) < 2 {
			continue // Can't detect divergence without multiple versions
		}

		// Check if replicas have divergent versions (different block counts or sets)
		if s.hasMetadataDivergence(allMetas) {
			log.Printf("[BackgroundMerge] Detected divergence in file %s (%s), merging...", fileID, filename)
			go s.performBackgroundMerge(fileID, filename, replicas)
		}
	}
}

// hasMetadataDivergence checks if metadata versions differ across replicas
func (s *Server) hasMetadataDivergence(metas []*Metadata) bool {
	if len(metas) <= 1 {
		return false
	}

	// Compare block counts first (quick check)
	firstBlockCount := len(metas[0].Blocks)
	for i := 1; i < len(metas); i++ {
		if len(metas[i].Blocks) != firstBlockCount {
			return true // Different block counts = divergence
		}
	}

	// If same count, check if block IDs match
	firstBlockIDs := make(map[string]bool)
	for _, block := range metas[0].Blocks {
		firstBlockIDs[block.BlockID] = true
	}

	for i := 1; i < len(metas); i++ {
		for _, block := range metas[i].Blocks {
			if !firstBlockIDs[block.BlockID] {
				return true // Different blocks = divergence
			}
		}
	}

	return false // All replicas have identical metadata
}

// performBackgroundMerge executes merge logic automatically in the background
func (s *Server) performBackgroundMerge(fileID, filename string, replicas []utils.NodeID) {
	// Get metadata from all replicas WITH node tracking
	metaResponses := s.performGetMetadataWithNodes(replicas, fileID)
	if len(metaResponses) == 0 {
		return
	}

	// Collect all unique blocks from all metadata versions
	uniqueBlocks := make(map[string]BlockInfo)
	for _, resp := range metaResponses {
		if resp.Meta != nil {
			for _, block := range resp.Meta.Blocks {
				uniqueBlocks[block.BlockID] = block
			}
		}
	}

	// Create merged list
	mergedBlocks := make([]BlockInfo, 0, len(uniqueBlocks))
	for _, block := range uniqueBlocks {
		mergedBlocks = append(mergedBlocks, block)
	}

	// Sort by (ClientID, Timestamp) to preserve per-client ordering
	sort.SliceStable(mergedBlocks, func(i, j int) bool {
		if mergedBlocks[i].ClientID == mergedBlocks[j].ClientID {
			return mergedBlocks[i].Timestamp < mergedBlocks[j].Timestamp
		}
		return mergedBlocks[i].ClientID < mergedBlocks[j].ClientID
	})

	goldenMeta := &Metadata{
		FileID:   fileID,
		Filename: filename,
		Blocks:   mergedBlocks,
	}

	// Compare each replica's metadata against the golden version
	outdatedReplicas := make([]utils.NodeID, 0)
	for _, resp := range metaResponses {
		if resp.Meta != nil && !s.isMetadataUpToDate(resp.Meta, goldenMeta) {
			outdatedReplicas = append(outdatedReplicas, resp.Node)
		}
	}

	// If no outdated replicas found, everyone is already in sync
	if len(outdatedReplicas) == 0 {
		log.Printf("[BackgroundMerge] All replicas already in sync for file %s (%s)", fileID, filename)
		return
	}

	// Propagate golden metadata ONLY to outdated replicas (bandwidth optimization)
	ackCount := s.dispatchWriteMetas(outdatedReplicas, fileID, goldenMeta)
	log.Printf("[BackgroundMerge] Merged file %s (%s), updated %d/%d outdated replicas (skipped %d already in sync)",
		fileID, filename, ackCount, len(outdatedReplicas), len(replicas)-len(outdatedReplicas))
}

// performGetMetadataWithNodes fetches metadata from replicas and tracks which node has which version
func (s *Server) performGetMetadataWithNodes(replicas []utils.NodeID, fileID string) []metaResponse {
	var wg sync.WaitGroup
	respChan := make(chan metaResponse, len(replicas))

	for _, replica := range replicas {
		wg.Add(1)
		go func(node utils.NodeID) {
			defer wg.Done()
			url := s.buildReplicaURL(node, fmt.Sprintf("/internal/get-meta?fileid=%s", fileID))
			resp, err := s.Client.Get(url)

			if err == nil && resp.StatusCode == http.StatusOK {
				var meta Metadata
				if decodeErr := json.NewDecoder(resp.Body).Decode(&meta); decodeErr == nil {
					respChan <- metaResponse{Meta: &meta, Node: node, Err: nil}
				} else {
					respChan <- metaResponse{Meta: nil, Node: node, Err: decodeErr}
				}
				resp.Body.Close()
			} else {
				if resp != nil {
					resp.Body.Close()
				}
				respChan <- metaResponse{Meta: nil, Node: node, Err: err}
			}
		}(replica)
	}

	wg.Wait()
	close(respChan)

	responses := make([]metaResponse, 0, len(replicas))
	for resp := range respChan {
		if resp.Meta != nil { // Only include successful responses
			responses = append(responses, resp)
		}
	}
	return responses
}

// isMetadataUpToDate checks if a replica's metadata matches the golden version
func (s *Server) isMetadataUpToDate(replicaMeta, goldenMeta *Metadata) bool {
	if len(replicaMeta.Blocks) != len(goldenMeta.Blocks) {
		return false
	}

	// Create a set of golden block IDs for quick lookup
	goldenBlockIDs := make(map[string]bool)
	for _, block := range goldenMeta.Blocks {
		goldenBlockIDs[block.BlockID] = true
	}

	// Check if replica has all golden blocks
	for _, block := range replicaMeta.Blocks {
		if !goldenBlockIDs[block.BlockID] {
			return false
		}
	}

	return true // Replica is up-to-date
}

// getFileID is a helper to hash the filename
func (s *Server) getFileID(hydfsFilename string) string {
	return fmt.Sprintf("%x", s.ring.Hash(hydfsFilename))
}

// buildReplicaURL constructs the full HTTP URL for an internal API call
func (s *Server) buildReplicaURL(node utils.NodeID, path string) string {
	// We use the node's IP and the *known* ControlPort
	return fmt.Sprintf("http://%s:%d%s", node.IP, s.ControlPort, path)
}

// ###########################################################################
// ### 1. PUBLIC HANDLERS (COORDINATOR LOGIC)                            ###
// ###########################################################################

// HandleCreate handles the 'create' command
func (s *Server) HandleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	hydfsFilename := r.FormValue("hydfsfile")
	if hydfsFilename == "" {
		http.Error(w, "Missing 'hydfsfile' form value", http.StatusBadRequest)
		return
	}

	// Extract client identity (IP:Port from remote address)
	clientID := r.RemoteAddr
	if clientID == "" {
		clientID = "unknown_client"
	}

	fileID := s.getFileID(hydfsFilename)
	log.Printf("[HyDFS] Received /create for %s (ID: %s) from client %s", hydfsFilename, fileID, clientID)

	file, header, err := r.FormFile("localfile")
	if err != nil {
		http.Error(w, "Missing 'localfile' form file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// 1. Find replicas
	replicas := s.ring.GetSuccessors(hydfsFilename, 3)
	if len(replicas) < 3 {
		http.Error(w, "Failed to find 3 alive replicas", http.StatusServiceUnavailable)
		return
	}

	// 2. Create block info and metadata
	timestamp := time.Now().UnixNano()
	blockInfo := BlockInfo{
		BlockID:   fmt.Sprintf("%s_%d", clientID, timestamp),
		ClientID:  clientID,
		Timestamp: timestamp,
		Size:      header.Size,
	}
	meta := &Metadata{
		FileID:   fileID,
		Filename: hydfsFilename,
		Blocks:   []BlockInfo{blockInfo},
	}
	metaBytes, _ := json.Marshal(meta)

	// 3. Read file data into memory so it can be sent multiple times
	var fileData bytes.Buffer
	if _, err := io.Copy(&fileData, file); err != nil {
		http.Error(w, "Failed to read file data", http.StatusInternalServerError)
		return
	}

	// 4. Fan-out write to replicas
	ackCount := s.dispatchWrites(replicas, fileID, blockInfo, fileData.Bytes(), metaBytes)

	// 5. Check for Write Quorum (W=3) - Strong consistency for initial file creation
	if ackCount < 3 {
		log.Printf("[HyDFS] Create for %s FAILED quorum (W=%d)", hydfsFilename, ackCount)
		http.Error(w, fmt.Sprintf("Write quorum failed (W=%d, required W=3)", ackCount), http.StatusServiceUnavailable)
		return
	}

	log.Printf("[HyDFS] Create for %s completed (W=%d)", hydfsFilename, ackCount)
	fmt.Fprintf(w, "File %s created successfully on replicas (W=%d & FileID=%s):\n", hydfsFilename, ackCount, fileID)
	for _, rep := range replicas {
		fmt.Fprintf(w, "  - %s\n", rep.Address())
	}
}

// HandleAppend handles the 'append' command
func (s *Server) HandleAppend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	hydfsFilename := r.FormValue("hydfsfile")
	if hydfsFilename == "" {
		http.Error(w, "Missing 'hydfsfile' form value", http.StatusBadRequest)
		return
	}

	// Extract client identity (IP:Port from remote address)
	clientID := r.RemoteAddr
	if clientID == "" {
		clientID = "unknown_client"
	}

	fileID := s.getFileID(hydfsFilename)
	log.Printf("[HyDFS] Received /append for %s (ID: %s) from client %s", hydfsFilename, fileID, clientID)

	file, header, err := r.FormFile("localfile")
	if err != nil {
		http.Error(w, "Missing 'localfile' form file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// 1. Find replicas
	replicas := s.ring.GetSuccessors(hydfsFilename, 3)
	if len(replicas) < 3 {
		http.Error(w, "Failed to find 3 alive replicas", http.StatusServiceUnavailable)
		return
	}

	// 2. *** READ QUORUM (R=2) FIRST ***
	// We must get the latest metadata before appending to satisfy consistency.
	allMetas := s.performGetMetadata(replicas, fileID)
	if len(allMetas) < 2 {
		log.Printf("[HyDFS] Append for %s FAILED read quorum (R=%d)", hydfsFilename, len(allMetas))
		http.Error(w, fmt.Sprintf("Read quorum failed (R=%d, required R=2) to find file for append", len(allMetas)), http.StatusServiceUnavailable)
		return
	}
	winningMeta := s.findWinningMetadata(allMetas)
	if winningMeta.Filename == "" {
		winningMeta.Filename = hydfsFilename
	}

	// 3. Create new BlockInfo and update the metadata
	timestamp := time.Now().UnixNano()
	blockInfo := BlockInfo{
		BlockID:   fmt.Sprintf("%s_%d", clientID, timestamp),
		ClientID:  clientID,
		Timestamp: timestamp,
		Size:      header.Size,
	}
	winningMeta.Blocks = append(winningMeta.Blocks, blockInfo)
	metaBytes, _ := json.Marshal(winningMeta)

	// 4. Read file data into memory
	var fileData bytes.Buffer
	if _, err := io.Copy(&fileData, file); err != nil {
		http.Error(w, "Failed to read file data", http.StatusInternalServerError)
		return
	}

	// 5. Fan-out write to replicas
	ackCount := s.dispatchWrites(replicas, fileID, blockInfo, fileData.Bytes(), metaBytes)

	// 6. Check for Write Quorum (W=2) - Eventual consistency per spec (W=2, R=2)
	if ackCount < 2 {
		log.Printf("[HyDFS] Append for %s FAILED write quorum (W=%d)", hydfsFilename, ackCount)
		http.Error(w, fmt.Sprintf("Write quorum failed (W=%d, required W=2)", ackCount), http.StatusServiceUnavailable)
		return
	}

	log.Printf("[HyDFS] Append for %s completed (W=%d)", hydfsFilename, ackCount)
	fmt.Fprintf(w, "File %s appended successfully on replicas (W=%d):\n", hydfsFilename, ackCount)
	for _, rep := range replicas {
		fmt.Fprintf(w, "  - %s\n", rep.Address())
	}
}

// HandleGet handles the 'get' command
func (s *Server) HandleGet(w http.ResponseWriter, r *http.Request) {
	hydfsFilename := r.URL.Query().Get("hydfsfile")
	if hydfsFilename == "" {
		http.Error(w, "Missing 'hydfsfile' query param", http.StatusBadRequest)
		return
	}
	fileID := s.getFileID(hydfsFilename)
	log.Printf("[HyDFS] Received /get for %s (ID: %s)", hydfsFilename, fileID)

	// 1. Find replicas
	replicas := s.ring.GetSuccessors(hydfsFilename, 3)
	if len(replicas) < 3 {
		http.Error(w, "Failed to find 3 alive replicas", http.StatusServiceUnavailable)
		return
	}

	// 2. Perform Read Quorum (R=2) - Eventual consistency per spec (W=2, R=2)
	allMetas := s.performGetMetadata(replicas, fileID)
	if len(allMetas) < 2 {
		log.Printf("[HyDFS] Get for %s FAILED read quorum (R=%d)", hydfsFilename, len(allMetas))
		http.Error(w, fmt.Sprintf("Read quorum failed (R=%d, required R=2). File not found or replicas down.", len(allMetas)), http.StatusNotFound)
		return
	}
	winningMeta := s.findWinningMetadata(allMetas)

	// 3. Perform "Read-Repair" in the background (asynchronous)
	// Send the winning metadata to all replicas.
	go s.dispatchWriteMetas(replicas, fileID, winningMeta)

	// 4. Stream blocks in order to the client
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", hydfsFilename))

	for _, blockInfo := range winningMeta.Blocks {
		var blockData io.ReadCloser
		fetched := false

		// Try each replica until we find one that has the block
		for _, replica := range replicas {
			url := s.buildReplicaURL(replica, fmt.Sprintf("/internal/get-block?fileid=%s&blockid=%s", fileID, blockInfo.BlockID))
			resp, err := s.Client.Get(url)
			if err == nil && resp.StatusCode == http.StatusOK {
				blockData = resp.Body
				fetched = true
				break // Found the block
			}
			if resp != nil {
				resp.Body.Close()
			}
			log.Printf("[HyDFS] Get block %s failed from replica %s: %v", blockInfo.BlockID, replica.Address(), err)
		}

		if !fetched {
			log.Printf("[HyDFS] Get for %s FAILED: Could not find block %s on any replica", hydfsFilename, blockInfo.BlockID)
			http.Error(w, "Failed to retrieve block "+blockInfo.BlockID, http.StatusInternalServerError)
			return
		}

		if _, err := io.Copy(w, blockData); err != nil {
			log.Printf("[HyDFS] Error streaming block: %v", err)
			blockData.Close()
			return // Client disconnected
		}
		blockData.Close()
	}
	log.Printf("[HyDFS] Get for %s completed", hydfsFilename)
}

// HandleMerge handles the 'merge' command
func (s *Server) HandleMerge(w http.ResponseWriter, r *http.Request) {
	hydfsFilename := r.URL.Query().Get("hydfsfile")
	if hydfsFilename == "" {
		http.Error(w, "Missing 'hydfsfile' query param", http.StatusBadRequest)
		return
	}
	fileID := s.getFileID(hydfsFilename)
	log.Printf("[HyDFS] Received /merge for %s (ID: %s)", hydfsFilename, fileID)

	// 1. Find all N=3 replicas
	replicas := s.ring.GetSuccessors(hydfsFilename, 3)
	if len(replicas) < 3 {
		http.Error(w, "Failed to find 3 alive replicas", http.StatusServiceUnavailable)
		return
	}

	// 2. Get metadata from ALL replicas
	allMetas := s.performGetMetadata(replicas, fileID)
	if len(allMetas) == 0 {
		http.Error(w, "File not found on any replica", http.StatusNotFound)
		return
	}

	// 3. Perform merge logic
	//    a. Collect all unique blocks from all metadata versions
	uniqueBlocks := make(map[string]BlockInfo)
	for _, meta := range allMetas {
		for _, block := range meta.Blocks {
			uniqueBlocks[block.BlockID] = block
		}
	}
	//    b. Create a list of all unique blocks
	mergedBlocks := make([]BlockInfo, 0, len(uniqueBlocks))
	for _, block := range uniqueBlocks {
		mergedBlocks = append(mergedBlocks, block)
	}

	//    c. Sort to satisfy per-client append ordering
	sort.SliceStable(mergedBlocks, func(i, j int) bool {
		if mergedBlocks[i].ClientID == mergedBlocks[j].ClientID {
			return mergedBlocks[i].Timestamp < mergedBlocks[j].Timestamp
		}
		return mergedBlocks[i].ClientID < mergedBlocks[j].ClientID
	})

	goldenMeta := &Metadata{
		FileID:   fileID,
		Filename: hydfsFilename,
		Blocks:   mergedBlocks,
	}

	// 4. Propagate "golden" metadata to ALL replicas
	ackCount := s.dispatchWriteMetas(replicas, fileID, goldenMeta)

	// 5. Wait for successful completion from ALL N=3 replicas
	if ackCount < len(replicas) {
		log.Printf("[HyDFS] Merge for %s FAILED: Not all replicas ACKed (ACKs: %d)", hydfsFilename, ackCount)
		http.Error(w, fmt.Sprintf("Merge failed, only %d/%d replicas acknowledged", ackCount, len(replicas)), http.StatusInternalServerError)
		return
	}

	log.Printf("[HyDFS] Merge for %s completed", hydfsFilename)
	fmt.Fprintf(w, "File %s merged successfully across all %d replicas.\n", hydfsFilename, ackCount)
}

// HandleLs handles the 'ls' command
// Shows which VMs store the replicas of a given file
func (s *Server) HandleLs(w http.ResponseWriter, r *http.Request) {
	hydfsFilename := r.URL.Query().Get("hydfsfile")
	if hydfsFilename == "" {
		http.Error(w, "Missing 'hydfsfile' query param", http.StatusBadRequest)
		return
	}
	fileID := s.getFileID(hydfsFilename)
	log.Printf("[HyDFS] Received /ls for %s", hydfsFilename)

	replicas := s.ring.GetSuccessors(hydfsFilename, 3)

	fmt.Fprintf(w, "File: %s (FileID: %s)\n", hydfsFilename, fileID)
	fmt.Fprintln(w, "Replicas (n=3):")
	for _, replica := range replicas {
		fmt.Fprintf(w, "  - %s (RingID: %d)\n", replica.Address(), s.ring.Hash(replica.String()))
	}
}

// HandleListStore handles the 'liststore' command
func (s *Server) HandleListStore(w http.ResponseWriter, r *http.Request) {
	log.Printf("[HyDFS] Received /liststore")

	fileIDs, err := s.store.ListLocalFiles()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	selfHash := s.ring.Hash(s.selfNodeID.String())
	fmt.Fprintf(w, "Node: %s (RingID: %d)\n", s.selfNodeID.Address(), selfHash)
	fmt.Fprintln(w, "Stored files:")
	if len(fileIDs) == 0 {
		fmt.Fprintln(w, "  (None)")
		return
	}
	for _, fileID := range fileIDs {
		meta, metaErr := s.store.ReadMetadata(fileID)
		filename := "(unknown)"
		if metaErr != nil {
			log.Printf("[HyDFS] liststore: failed to read metadata for %s: %v", fileID, metaErr)
		} else if meta != nil && meta.Filename != "" {
			filename = meta.Filename
		}
		fmt.Fprintf(w, "  - %s (FileID: %s)\n", filename, fileID)
	}
}

// **** THIS IS THE FIX ****
// GetNodeHashes is the pass-through method for list_mem_ids
func (s *Server) GetNodeHashes() map[string]uint32 {
	return s.ring.GetNodeHashes()
}

// ###########################################################################
// ### 2. INTERNAL HANDLERS (REPLICA LOGIC)                              ###
// ###########################################################################

// HandleInternalWrite saves a block and its associated metadata
func (s *Server) HandleInternalWrite(w http.ResponseWriter, r *http.Request) {
	fileID := r.FormValue("fileid")
	metaJSON := r.FormValue("metadata")
	blockInfoJSON := r.FormValue("blockinfo")
	if fileID == "" || metaJSON == "" || blockInfoJSON == "" {
		http.Error(w, "Missing form fields: fileid, metadata, blockinfo", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("blockdata")
	if err != nil {
		http.Error(w, "Missing 'blockdata' form file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	var meta Metadata
	var blockInfo BlockInfo
	if err := json.Unmarshal([]byte(metaJSON), &meta); err != nil {
		http.Error(w, "Invalid metadata JSON", http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal([]byte(blockInfoJSON), &blockInfo); err != nil {
		http.Error(w, "Invalid blockinfo JSON", http.StatusBadRequest)
		return
	}

	log.Printf("[HyDFS-Replica] RECEIVED write request for file %s (block: %s)", meta.Filename, blockInfo.BlockID)

	// 1. Write the block data
	if _, err := s.store.WriteBlock(fileID, blockInfo.BlockID, file); err != nil {
		log.Printf("[HyDFS-Replica] FAILED to write block %s: %v", blockInfo.BlockID, err)
		http.Error(w, "Failed to write block", http.StatusInternalServerError)
		return
	}

	// 2. Write the (new, complete) metadata
	if err := s.store.WriteMetadata(fileID, &meta); err != nil {
		log.Printf("[HyDFS-Replica] FAILED to write metadata for %s: %v", fileID, err)
		http.Error(w, "Failed to write metadata", http.StatusInternalServerError)
		return
	}

	log.Printf("[HyDFS-Replica] COMPLETED write for file %s (block: %s)", meta.Filename, blockInfo.BlockID)
	w.WriteHeader(http.StatusOK)
}

// HandleInternalGetMeta returns this node's version of a file's metadata
func (s *Server) HandleInternalGetMeta(w http.ResponseWriter, r *http.Request) {
	fileID := r.URL.Query().Get("fileid")
	if fileID == "" {
		http.Error(w, "Missing 'fileid' query param", http.StatusBadRequest)
		return
	}

	meta, err := s.store.ReadMetadata(fileID)
	if err != nil {
		if os.IsNotExist(err) {
			// This is not an error; the replica just doesn't have the file.
			// Return empty metadata so the coordinator can merge.
			log.Printf("[HyDFS-Replica] Metadata for %s not found, returning empty.", fileID)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(&Metadata{FileID: fileID, Blocks: []BlockInfo{}})
			return
		}
		log.Printf("[HyDFS-Replica] FAILED to read metadata for %s: %v", fileID, err)
		http.Error(w, "Failed to read metadata", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(meta)
}

// HandleInternalGetBlock returns a specific block's data
func (s *Server) HandleInternalGetBlock(w http.ResponseWriter, r *http.Request) {
	fileID := r.URL.Query().Get("fileid")
	blockID := r.URL.Query().Get("blockid")
	if fileID == "" || blockID == "" {
		http.Error(w, "Missing 'fileid' or 'blockid' query param", http.StatusBadRequest)
		return
	}

	log.Printf("[HyDFS-Replica] RECEIVED get-block request for file %s (block: %s)", fileID, blockID)

	file, err := s.store.ReadBlock(fileID, blockID)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[HyDFS-Replica] Block %s not found", blockID)
			http.Error(w, "Block not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to read block", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	io.Copy(w, file)
	log.Printf("[HyDFS-Replica] COMPLETED get-block for file %s (block: %s)", fileID, blockID)
}

// HandleInternalListFiles returns a list of all file IDs stored on this node
func (s *Server) HandleInternalListFiles(w http.ResponseWriter, r *http.Request) {
	fileIDs, err := s.store.ListLocalFiles()
	if err != nil {
		http.Error(w, "Failed to list files", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(fileIDs)
}

// HandleInternalWriteMeta overwrites the local metadata with a "golden" version (from merge/read-repair)
func (s *Server) HandleInternalWriteMeta(w http.ResponseWriter, r *http.Request) {
	fileID := r.URL.Query().Get("fileid")
	if fileID == "" {
		http.Error(w, "Missing 'fileid' query param", http.StatusBadRequest)
		return
	}

	var meta Metadata
	if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
		http.Error(w, "Invalid metadata JSON body", http.StatusBadRequest)
		return
	}

	if err := s.store.WriteMetadata(fileID, &meta); err != nil {
		log.Printf("[HyDFS-Replica] FAILED to write golden metadata for %s: %v", fileID, err)
		http.Error(w, "Failed to write metadata", http.StatusInternalServerError)
		return
	}

	log.Printf("[HyDFS-Replica] Successfully overwrote golden metadata for %s", fileID)
	w.WriteHeader(http.StatusOK)
}

// HandleInternalDelete removes a file from this replica (used for rebalancing)
func (s *Server) HandleInternalDelete(w http.ResponseWriter, r *http.Request) {
	fileID := r.URL.Query().Get("fileid")
	if fileID == "" {
		http.Error(w, "Missing 'fileid' query param", http.StatusBadRequest)
		return
	}

	if err := s.store.DeleteFile(fileID); err != nil {
		log.Printf("[HyDFS-Replica] FAILED to delete file %s: %v", fileID, err)
		http.Error(w, "Failed to delete file", http.StatusInternalServerError)
		return
	}

	log.Printf("[HyDFS-Replica] Successfully deleted file %s", fileID)
	w.WriteHeader(http.StatusOK)
}

// HandleInternalGetLocal fetches a file from LOCAL storage only (no quorum read)
// This is used by getfromreplica to inspect individual replica state
func (s *Server) HandleInternalGetLocal(w http.ResponseWriter, r *http.Request) {
	hydfsFilename := r.URL.Query().Get("hydfsfile")
	if hydfsFilename == "" {
		http.Error(w, "Missing 'hydfsfile' query param", http.StatusBadRequest)
		return
	}
	fileID := s.getFileID(hydfsFilename)
	log.Printf("[HyDFS-Replica] Received /internal/get-local for %s (ID: %s)", hydfsFilename, fileID)

	// Read LOCAL metadata only
	meta, err := s.store.ReadMetadata(fileID)
	if err != nil {
		log.Printf("[HyDFS-Replica] Failed to read local metadata for %s: %v", fileID, err)
		http.Error(w, "File not found locally", http.StatusNotFound)
		return
	}

	if len(meta.Blocks) == 0 {
		log.Printf("[HyDFS-Replica] No blocks found for %s locally", fileID)
		http.Error(w, "File not found locally", http.StatusNotFound)
		return
	}

	// Stream blocks from local storage
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", hydfsFilename))

	for _, blockInfo := range meta.Blocks {
		file, err := s.store.ReadBlock(fileID, blockInfo.BlockID)
		if err != nil {
			log.Printf("[HyDFS-Replica] Failed to read local block %s: %v", blockInfo.BlockID, err)
			http.Error(w, "Failed to read block from local storage", http.StatusInternalServerError)
			return
		}

		if _, err := io.Copy(w, file); err != nil {
			log.Printf("[HyDFS-Replica] Error streaming local block: %v", err)
			file.Close()
			return
		}
		file.Close()
	}
	log.Printf("[HyDFS-Replica] Successfully streamed local file %s", hydfsFilename)
}

// ###########################################################################
// ### 3. INTERNAL DISPATCH HELPERS (COORDINATOR LOGIC)                  ###
// ###########################################################################

// dispatchWrites fans out a block and metadata to a list of replicas
func (s *Server) dispatchWrites(replicas []utils.NodeID, fileID string, blockInfo BlockInfo, fileData []byte, metaBytes []byte) int {
	var wg sync.WaitGroup
	ackChan := make(chan bool, len(replicas))

	blockInfoBytes, _ := json.Marshal(blockInfo)

	for _, replica := range replicas {
		wg.Add(1)
		go func(node utils.NodeID) {
			defer wg.Done()
			url := s.buildReplicaURL(node, "/internal/write")

			body := &bytes.Buffer{}
			writer := multipart.NewWriter(body)
			writer.WriteField("fileid", fileID)
			writer.WriteField("metadata", string(metaBytes))
			writer.WriteField("blockinfo", string(blockInfoBytes))

			part, _ := writer.CreateFormFile("blockdata", blockInfo.BlockID)
			part.Write(fileData)
			writer.Close()

			req, _ := http.NewRequest(http.MethodPost, url, body)
			req.Header.Set("Content-Type", writer.FormDataContentType())

			resp, err := s.Client.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				ackChan <- true
				resp.Body.Close()
			} else {
				log.Printf("[HyDFS-Coord] dispatchWrite to %s failed: %v", node.Address(), err)
				ackChan <- false
				if resp != nil {
					resp.Body.Close()
				}
			}
		}(replica)
	}

	wg.Wait()
	close(ackChan)

	ackCount := 0
	for success := range ackChan {
		if success {
			ackCount++
		}
	}
	return ackCount
}

// performGetMetadata fans out to replicas to get their metadata versions
func (s *Server) performGetMetadata(replicas []utils.NodeID, fileID string) []*Metadata {
	var wg sync.WaitGroup
	metaChan := make(chan *Metadata, len(replicas))

	for _, replica := range replicas {
		wg.Add(1)
		go func(node utils.NodeID) {
			defer wg.Done()
			url := s.buildReplicaURL(node, fmt.Sprintf("/internal/get-meta?fileid=%s", fileID))
			resp, err := s.Client.Get(url)
			if err == nil && resp.StatusCode == http.StatusOK {
				var meta Metadata
				if err := json.NewDecoder(resp.Body).Decode(&meta); err == nil {
					metaChan <- &meta
				}
				resp.Body.Close()
			} else {
				log.Printf("[HyDFS-Coord] performGetMetadata from %s failed: %v", node.Address(), err)
				if resp != nil {
					resp.Body.Close()
				}
			}
		}(replica)
	}

	wg.Wait()
	close(metaChan)

	allMetas := make([]*Metadata, 0, len(replicas))
	for meta := range metaChan {
		allMetas = append(allMetas, meta)
	}
	return allMetas
}

// findWinningMetadata selects the "best" metadata from a list (e.g., for read-repair)
func (s *Server) findWinningMetadata(allMetas []*Metadata) *Metadata {
	if len(allMetas) == 0 {
		return nil
	}
	// Winner is the one with the most blocks.
	// A more robust system might use vector clocks.
	winner := allMetas[0]
	for _, meta := range allMetas[1:] {
		if len(meta.Blocks) > len(winner.Blocks) {
			winner = meta
		}
	}
	return winner
}

// dispatchWriteMetas fans out a "golden" metadata to all replicas (for merge/read-repair)
// Also replicates any missing blocks to ensure eventual consistency
func (s *Server) dispatchWriteMetas(replicas []utils.NodeID, fileID string, goldenMeta *Metadata) int {
	var wg sync.WaitGroup
	ackChan := make(chan bool, len(replicas))

	for _, replica := range replicas {
		wg.Add(1)
		go func(node utils.NodeID) {
			defer wg.Done()

			// Step 1: Get current metadata from this replica
			metaURL := s.buildReplicaURL(node, fmt.Sprintf("/internal/get-meta?fileid=%s", fileID))
			metaResp, err := s.Client.Get(metaURL)

			var replicaMeta Metadata
			if err == nil && metaResp.StatusCode == http.StatusOK {
				json.NewDecoder(metaResp.Body).Decode(&replicaMeta)
				metaResp.Body.Close()
			}

			// Step 2: Identify missing blocks
			replicaBlockIDs := make(map[string]bool)
			for _, block := range replicaMeta.Blocks {
				replicaBlockIDs[block.BlockID] = true
			}

			missingBlocks := make([]BlockInfo, 0)
			for _, block := range goldenMeta.Blocks {
				if !replicaBlockIDs[block.BlockID] {
					missingBlocks = append(missingBlocks, block)
				}
			}

			// Step 3: Replicate missing blocks from self or other replicas
			if len(missingBlocks) > 0 {
				log.Printf("[HyDFS-Coord] Replicating %d missing blocks to %s for file %s",
					len(missingBlocks), node.Address(), fileID)

				for _, block := range missingBlocks {
					// Try to fetch block from local storage first
					blockFile, err := s.store.ReadBlock(fileID, block.BlockID)
					var blockData []byte

					if err == nil {
						blockData, _ = io.ReadAll(blockFile)
						blockFile.Close()
					} else {
						// Block not local, fetch from another replica
						for _, sourceReplica := range replicas {
							if sourceReplica.String() == node.String() {
								continue
							}
							blockURL := s.buildReplicaURL(sourceReplica,
								fmt.Sprintf("/internal/get-block?fileid=%s&blockid=%s", fileID, block.BlockID))
							blockResp, blockErr := s.Client.Get(blockURL)
							if blockErr == nil && blockResp.StatusCode == http.StatusOK {
								blockData, _ = io.ReadAll(blockResp.Body)
								blockResp.Body.Close()
								break
							}
							if blockResp != nil {
								blockResp.Body.Close()
							}
						}
					}

					if blockData == nil {
						log.Printf("[HyDFS-Coord] Could not find block %s to replicate", block.BlockID)
						continue
					}

					// Write block to target replica
					body := &bytes.Buffer{}
					writer := multipart.NewWriter(body)
					writer.WriteField("fileid", fileID)
					metaBytes, _ := json.Marshal(goldenMeta)
					writer.WriteField("metadata", string(metaBytes))
					blockInfoBytes, _ := json.Marshal(block)
					writer.WriteField("blockinfo", string(blockInfoBytes))

					part, _ := writer.CreateFormFile("blockdata", block.BlockID)
					part.Write(blockData)
					writer.Close()

					writeURL := s.buildReplicaURL(node, "/internal/write")
					req, _ := http.NewRequest(http.MethodPost, writeURL, body)
					req.Header.Set("Content-Type", writer.FormDataContentType())

					resp, err := s.Client.Do(req)
					if err == nil && resp.StatusCode == http.StatusOK {
						resp.Body.Close()
					} else {
						log.Printf("[HyDFS-Coord] Failed to replicate block %s to %s: %v",
							block.BlockID, node.Address(), err)
						if resp != nil {
							resp.Body.Close()
						}
					}
				}
			}

			// Step 4: Update metadata
			metaBytes, _ := json.Marshal(goldenMeta)
			writeMetaURL := s.buildReplicaURL(node, fmt.Sprintf("/internal/write-meta?fileid=%s", fileID))
			req, _ := http.NewRequest(http.MethodPost, writeMetaURL, bytes.NewReader(metaBytes))
			req.Header.Set("Content-Type", "application/json")

			resp, err := s.Client.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				ackChan <- true
				resp.Body.Close()
			} else {
				log.Printf("[HyDFS-Coord] dispatchWriteMetas to %s failed: %v", node.Address(), err)
				ackChan <- false
				if resp != nil {
					resp.Body.Close()
				}
			}
		}(replica)
	}

	wg.Wait()
	close(ackChan)

	ackCount := 0
	for success := range ackChan {
		if success {
			ackCount++
		}
	}
	return ackCount
}
