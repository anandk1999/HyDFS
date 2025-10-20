package hydfs

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Store manages the local filesystem storage for HyDFS
type Store struct {
	sync.RWMutex
	BasePath string
}

// NewStore creates a new storage manager
func NewStore(basePath string) (*Store, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}
	return &Store{BasePath: basePath}, nil
}

// getFileDir returns the path to the directory for a given fileID
func (s *Store) getFileDir(fileID string) string {
	return filepath.Join(s.BasePath, fileID)
}

// getMetaPath returns the path to the metadata file
func (s *Store) getMetaPath(fileID string) string {
	return filepath.Join(s.getFileDir(fileID), "_meta.json")
}

// getBlockPath returns the path to a specific block
func (s *Store) getBlockPath(fileID, blockID string) string {
	return filepath.Join(s.getFileDir(fileID), blockID)
}

// ReadMetadata locks, reads, and decodes the _meta.json file
func (s *Store) ReadMetadata(fileID string) (*Metadata, error) {
	s.Lock() // Use full lock to coordinate with WriteMetadata
	defer s.Unlock()

	metaPath := s.getMetaPath(fileID)
	meta := &Metadata{FileID: fileID}

	f, err := os.Open(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, return empty metadata
			return meta, nil
		}
		return nil, err
	}
	defer f.Close()

	if err := json.NewDecoder(f).Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// WriteMetadata locks, encodes, and writes the _meta.json file
func (s *Store) WriteMetadata(fileID string, meta *Metadata) error {
	s.Lock()
	defer s.Unlock()

	if err := os.MkdirAll(s.getFileDir(fileID), 0755); err != nil {
		return err
	}

	metaPath := s.getMetaPath(fileID)
	f, err := os.Create(metaPath)
	if err != nil {
		return err
	}
	defer f.Close()

	return json.NewEncoder(f).Encode(meta)
}

// WriteBlock streams data from an io.Reader to a block file
func (s *Store) WriteBlock(fileID, blockID string, data io.Reader) (int64, error) {
	s.Lock() // Lock to prevent conflicts while creating dir
	if err := os.MkdirAll(s.getFileDir(fileID), 0755); err != nil {
		s.Unlock()
		return 0, err
	}
	s.Unlock()

	blockPath := s.getBlockPath(fileID, blockID)
	f, err := os.Create(blockPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return io.Copy(f, data)
}

// ReadBlock opens a block file for reading
func (s *Store) ReadBlock(fileID, blockID string) (*os.File, error) {
	blockPath := s.getBlockPath(fileID, blockID)
	return os.Open(blockPath)
}

// ListLocalFiles lists all fileIDs (directories) stored locally
func (s *Store) ListLocalFiles() ([]string, error) {
	entries, err := os.ReadDir(s.BasePath)
	if err != nil {
		return nil, err
	}

	var fileIDs []string
	for _, entry := range entries {
		if entry.IsDir() {
			fileIDs = append(fileIDs, entry.Name())
		}
	}
	return fileIDs, nil
}

// DeleteFile completely removes a file's directory and all blocks
func (s *Store) DeleteFile(fileID string) error {
	s.Lock()
	defer s.Unlock()
	return os.RemoveAll(s.getFileDir(fileID))
}
