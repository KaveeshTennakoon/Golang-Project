package config

import "time"

// Timing constants tuned for local tests.
const (
	// Increased election timeouts for more stability
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 600 * time.Millisecond
	HeartbeatInterval  = 50 * time.Millisecond // More frequent heartbeats

	// Log pruning settings
	RetainTail = 100  // Keep at least this many entries after pruning
	PruneEvery = 2000 // Prune when log exceeds this many entries
)
