package rdb

import "sync"

var (
	rdbFile  []byte
	rdbMutex sync.Mutex
)

// SetRDBFile stores the RDB content received from the master.
func SetRDBFile(file []byte) {
	rdbMutex.Lock()
	defer rdbMutex.Unlock()
	rdbFile = file
}

// Additional RDB parsing functions like getKeys and getValue should be added here
// if you need to load data from a disk-based RDB file.