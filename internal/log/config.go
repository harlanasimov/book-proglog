package log

import "github.com/hashicorp/raft"

type Config struct {
	Segment struct {
		MaxStoreBytes int64
		MaxIndexBytes int64
		InitialOffset uint64
	}
	Raft struct {
		raft.Config
		StreamLayer *StreamLayer
		Bootstrap   bool
	}
}
