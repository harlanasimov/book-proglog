package log

type Config struct {
	Segment struct {
		MaxStoreBytes int64
		MaxIndexBytes int64
		InitialOffset uint64
	}
}
