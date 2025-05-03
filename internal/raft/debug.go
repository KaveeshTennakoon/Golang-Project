package raft

import (
	"fmt"
	"os"
)

// DebugLog prints debug information to stderr.
// Set debugEnabled to true to enable debug logging.
var debugEnabled = false

func DebugLog(format string, args ...interface{}) {
	if debugEnabled {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}
