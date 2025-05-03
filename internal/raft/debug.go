package raft

import (
	"fmt"
	"os"
)

// DebugLog prints debug information to stderr.
// Set debugEnabled to true to enable debug logging.
var debugEnabled = true

func DebugLog(format string, args ...any) {
	if debugEnabled {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}
