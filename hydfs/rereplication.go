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
	"sync"
	"sync/atomic"
)

// checkForReReplication monitors files stored locally and ensures they maintain N=3 replicas
// This is called periodically from backgroundTasks()
func (s *Server) checkForReReplication() {
	s.runReReplication("background")
}

// TriggerReReplication allows external observers (e.g., failure detector) to kick off
// an immediate re-replication attempt.
func (s *Server) TriggerReReplication(reason string) {
	go s.runReReplication(reason)
}

// runReReplication serialises concurrent requests and executes the re-replication scan.
func (s *Server) runReReplication(reason string) {
	if !atomic.CompareAndSwapInt32(&s.replState, 0, 1) {
		log.Printf("[ReReplication] Skipping trigger (%s): scan already in progress", reason)
		return
	}
	defer atomic.StoreInt32(&s.replState, 0)

	s.performReReplication(reason)
}

func (s *Server) performReReplication(reason string) {
	fileIDs, err := s.store.ListLocalFiles()
	if err != nil {
		log.Printf("[ReReplication] Error listing local files (reason=%s): %v", reason, err)
		return
	}

	for _, fileID := range fileIDs {
		meta, err := s.store.ReadMetadata(fileID)
		if err != nil {
			continue
		}

		currentReplicas := s.findCurrentReplicasForFile(fileID)

		if len(currentReplicas) < 3 {
			log.Printf("[ReReplication] File %s replicas below quorum (%d/3), reason=%s", fileID, len(currentReplicas), reason)
			s.reReplicateFile(fileID, meta, currentReplicas)
		}
	}
}

// findCurrentReplicasForFile queries all alive nodes to see which ones have the file
func (s *Server) findCurrentReplicasForFile(fileID string) []utils.NodeID {
	members := s.membership.GetAllMembers()
	var wg sync.WaitGroup
	replicaChan := make(chan utils.NodeID, len(members))

	for _, member := range members {
		if member.Status != utils.Alive {
			continue
		}
		wg.Add(1)
		go func(node utils.NodeID) {
			defer wg.Done()
			url := s.buildReplicaURL(node, fmt.Sprintf("/internal/get-meta?fileid=%s", fileID))
			resp, err := s.Client.Get(url)
			if err == nil && resp.StatusCode == http.StatusOK {
				var meta Metadata
				if err := json.NewDecoder(resp.Body).Decode(&meta); err == nil {
					if len(meta.Blocks) > 0 {
						replicaChan <- node
					}
				}
				resp.Body.Close()
			}
		}(member.ID)
	}

	wg.Wait()
	close(replicaChan)

	replicas := make([]utils.NodeID, 0)
	for node := range replicaChan {
		replicas = append(replicas, node)
	}
	return replicas
}

func (s *Server) resolveFilename(meta *Metadata) string {
	if meta.Filename != "" {
		return meta.Filename
	}
	return meta.FileID
}

func (s *Server) shouldCoordinateReReplication(current []utils.NodeID) bool {
	if len(current) == 0 {
		return false
	}

	selfID := s.selfNodeID.String()
	lowest := ""
	foundSelf := false

	for idx, node := range current {
		nodeID := node.String()
		if idx == 0 || nodeID < lowest {
			lowest = nodeID
		}
		if nodeID == selfID {
			foundSelf = true
		}
	}

	return foundSelf && selfID == lowest
}

// reReplicateFile copies a file from existing replicas to new nodes to maintain N=3
func (s *Server) reReplicateFile(fileID string, meta *Metadata, currentReplicas []utils.NodeID) {
	if len(currentReplicas) >= 3 {
		return
	}

	if !s.shouldCoordinateReReplication(currentReplicas) {
		return
	}

	// Refresh metadata to ensure we replicate the most recent version
	freshMetas := s.performGetMetadata(currentReplicas, fileID)
	if len(freshMetas) > 0 {
		meta = s.findWinningMetadata(freshMetas)
	}

	filename := s.resolveFilename(meta)
	desiredReplicas := s.ring.GetSuccessors(filename, 3)
	if len(desiredReplicas) == 0 {
		log.Printf("[ReReplication] No desired replicas found for file %s (filename=%s)", fileID, filename)
		return
	}

	currentReplicaMap := make(map[string]bool)
	for _, node := range currentReplicas {
		currentReplicaMap[node.String()] = true
	}

	targetNodes := make([]utils.NodeID, 0)
	for _, desired := range desiredReplicas {
		if !currentReplicaMap[desired.String()] {
			targetNodes = append(targetNodes, desired)
		}
	}

	if len(targetNodes) == 0 {
		log.Printf("[ReReplication] File %s already present on desired successors", fileID)
		return
	}

	// Must have at least one source replica to copy from
	if len(currentReplicas) == 0 {
		log.Printf("[ReReplication] ERROR: No current replicas found for file %s", fileID)
		return
	}

	sourceNode := currentReplicas[0]
	log.Printf("[ReReplication] Copying file %s (%s) from %s to %d ring successors", fileID, filename, sourceNode.Address(), len(targetNodes))

	// Copy each block from source to all target nodes
	for _, block := range meta.Blocks {
		// Fetch block from source replica
		url := s.buildReplicaURL(sourceNode, fmt.Sprintf("/internal/get-block?fileid=%s&blockid=%s", fileID, block.BlockID))
		resp, err := s.Client.Get(url)
		if err != nil || resp.StatusCode != http.StatusOK {
			log.Printf("[ReReplication] Failed to fetch block %s from source %s: %v", block.BlockID, sourceNode.Address(), err)
			if resp != nil {
				resp.Body.Close()
			}
			continue
		}

		// Read block data into memory
		blockData, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("[ReReplication] Failed to read block %s data: %v", block.BlockID, err)
			continue
		}

		// Prepare metadata and block info for replication
		metaBytes, _ := json.Marshal(meta)
		blockInfoBytes, _ := json.Marshal(block)

		// Send block to each target node
		var wg sync.WaitGroup
		for _, targetNode := range targetNodes {
			wg.Add(1)
			go func(node utils.NodeID, data []byte) {
				defer wg.Done()

				body := &bytes.Buffer{}
				writer := multipart.NewWriter(body)
				writer.WriteField("fileid", fileID)
				writer.WriteField("metadata", string(metaBytes))
				writer.WriteField("blockinfo", string(blockInfoBytes))

				part, _ := writer.CreateFormFile("blockdata", block.BlockID)
				part.Write(data)
				writer.Close()

				url := s.buildReplicaURL(node, "/internal/write")
				req, _ := http.NewRequest(http.MethodPost, url, body)
				req.Header.Set("Content-Type", writer.FormDataContentType())

				resp, err := s.Client.Do(req)
				if err == nil && resp.StatusCode == http.StatusOK {
					log.Printf("[ReReplication] Successfully replicated block %s of file %s to %s", block.BlockID, fileID, node.Address())
					resp.Body.Close()
				} else {
					log.Printf("[ReReplication] Failed to replicate block %s to %s: %v", block.BlockID, node.Address(), err)
					if resp != nil {
						resp.Body.Close()
					}
				}
			}(targetNode, blockData)
		}
		wg.Wait()
	}

	log.Printf("[ReReplication] Completed re-replication of file %s to %d new nodes", fileID, len(targetNodes))
}
