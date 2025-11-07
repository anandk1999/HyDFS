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
	"time"
)

// checkForReReplication monitors files stored locally and ensures they maintain N=3 replicas
// This is called periodically from backgroundTasks()
func (s *Server) checkForReReplication() {
	s.runReReplication("background")
}

// TriggerReReplication allows external observers (e.g., failure detector) to kick off
// an immediate re-replication attempt.
func (s *Server) TriggerReReplication(reason string) {
	// Add brief delay before starting re-replication to let membership stabilize
	// This prevents flooding when multiple failure events occur simultaneously
	if reason != "background" {
		log.Printf("[ReReplication] Delaying re-replication for 1s (reason=%s) to stabilize membership", reason)
		time.Sleep(1 * time.Second) // Reduced from 3s - just enough to batch events
	}
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

	for i, fileID := range fileIDs {
		// Add throttling between file re-replications to prevent network flooding
		if i > 0 {
			// Small delay between files to pace requests
			if reason != "background" {
				time.Sleep(200 * time.Millisecond) // Reduced from 1s
			} else if i%5 == 0 {
				time.Sleep(100 * time.Millisecond) // Brief pause every 5 files
			}
		}

		meta, err := s.store.ReadMetadata(fileID)
		if err != nil {
			continue
		}

		// REQUIREMENT: Files must be stored on EXACTLY the first N=3 ring successors
		// We must enforce this invariant at all times
		s.enforceRingPlacement(fileID, meta, reason)
	}
}

// enforceRingPlacement ensures file is on exactly the first N=3 ring successors
// This handles both under-replication (< 3) and incorrect placement
func (s *Server) enforceRingPlacement(fileID string, meta *Metadata, reason string) {
	filename := s.resolveFilename(meta)

	// Get the CORRECT ring successors for this file
	desiredReplicas := s.ring.GetSuccessors(filename, 3)
	if len(desiredReplicas) == 0 {
		log.Printf("[ReReplication] No ring successors found for file %s", fileID)
		return
	}

	// Find which nodes CURRENTLY have the file
	currentReplicas := s.findCurrentReplicasForFile(fileID)

	// Only coordinate from lexicographically lowest current replica
	if len(currentReplicas) > 0 && !s.shouldCoordinateReReplication(currentReplicas) {
		return
	}

	// Build maps for comparison
	desiredMap := make(map[string]bool)
	for _, node := range desiredReplicas {
		desiredMap[node.String()] = true
	}

	currentMap := make(map[string]bool)
	for _, node := range currentReplicas {
		currentMap[node.String()] = true
	}

	// Find nodes that SHOULD have it but DON'T (need to copy TO)
	missingNodes := make([]utils.NodeID, 0)
	for _, desired := range desiredReplicas {
		if !currentMap[desired.String()] {
			missingNodes = append(missingNodes, desired)
		}
	}

	// Find nodes that HAVE it but SHOULDN'T (need to delete FROM)
	excessNodes := make([]utils.NodeID, 0)
	for _, current := range currentReplicas {
		if !desiredMap[current.String()] {
			excessNodes = append(excessNodes, current)
		}
	}

	// Log what we're doing
	if len(missingNodes) > 0 || len(excessNodes) > 0 {
		log.Printf("[ReReplication] File %s (%s): current=%d, desired=%d, missing=%d, excess=%d, reason=%s",
			fileID, filename, len(currentReplicas), len(desiredReplicas), len(missingNodes), len(excessNodes), reason)
	}

	// Step 1: Copy to missing nodes (if we have any source replicas)
	if len(missingNodes) > 0 {
		if len(currentReplicas) == 0 {
			log.Printf("[ReReplication] ERROR: File %s has no replicas to copy from", fileID)
			return
		}
		s.copyFileToMissingNodes(fileID, meta, currentReplicas[0], missingNodes)
	}

	// Step 2: Delete from excess nodes
	if len(excessNodes) > 0 {
		s.deleteFileFromNodes(fileID, excessNodes)
	}
}

// findCurrentReplicasForFile queries all alive nodes to see which ones have the file
func (s *Server) findCurrentReplicasForFile(fileID string) []utils.NodeID {
	members := s.membership.GetAllMembers()
	var wg sync.WaitGroup
	replicaChan := make(chan utils.NodeID, len(members))

	// CRITICAL: Very limited concurrency to avoid overwhelming network
	// Even 5 concurrent HTTP requests during mass re-replication can flood the network
	semaphore := make(chan struct{}, 2) // Reduced to max 2 concurrent checks

	for _, member := range members {
		if member.Status != utils.Alive {
			continue
		}
		wg.Add(1)
		go func(node utils.NodeID) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Add small delay between checks to pace requests
			time.Sleep(10 * time.Millisecond) // Reduced from 50ms

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
// copyFileToMissingNodes copies all blocks of a file from sourceNode to targetNodes
func (s *Server) copyFileToMissingNodes(fileID string, meta *Metadata, sourceNode utils.NodeID, targetNodes []utils.NodeID) {
	if len(targetNodes) == 0 {
		return
	}

	// Refresh metadata to ensure we replicate the most recent version
	freshMetas := s.performGetMetadata([]utils.NodeID{sourceNode}, fileID)
	if len(freshMetas) > 0 {
		meta = s.findWinningMetadata(freshMetas)
	}

	filename := s.resolveFilename(meta)
	log.Printf("[ReReplication] Copying file %s (%s) from %s to %d nodes", fileID, filename, sourceNode.Address(), len(targetNodes))

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

		// Send block to each target node with throttling between sends
		var wg sync.WaitGroup
		for idx, targetNode := range targetNodes {
			// Add small delay between replica sends to avoid network bursts
			if idx > 0 {
				time.Sleep(20 * time.Millisecond) // Reduced from 100ms
			}

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

// deleteFileFromNodes removes a file from nodes that should no longer store it
func (s *Server) deleteFileFromNodes(fileID string, nodes []utils.NodeID) {
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n utils.NodeID) {
			defer wg.Done()

			url := s.buildReplicaURL(n, fmt.Sprintf("/internal/delete?fileid=%s", fileID))
			req, _ := http.NewRequest(http.MethodDelete, url, nil)
			resp, err := s.Client.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				log.Printf("[Rebalance] Deleted file %s from %s", fileID, n.Address())
				resp.Body.Close()
			} else {
				log.Printf("[Rebalance] Failed to delete file %s from %s: %v", fileID, n.Address(), err)
				if resp != nil {
					resp.Body.Close()
				}
			}
		}(node)
	}
	wg.Wait()
}
