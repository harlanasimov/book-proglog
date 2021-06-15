package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/wenyuangui/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, l *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, l *Log) {
	want := &api.Record{Value: []byte("hello, world")}
	off, err := l.Append(want)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	got, err := l.Read(off)
	require.NoError(t, err)
	require.Equal(t, want.Value, got.Value)
}

func testOutOfRangeErr(t *testing.T, l *Log) {
	got, err := l.Read(1)
	require.Nil(t, got)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, l *Log) {
	want := &api.Record{Value: []byte("hello, world")}
	for i := 0; i < 3; i++ {
		_, err := l.Append(want)
		require.NoError(t, err)
	}

	off, err := l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	require.NoError(t, l.Close())

	n, err := NewLog(l.Dir, l.Config)
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, l *Log) {
	want := &api.Record{Value: []byte("hello, world")}
	off, err := l.Append(want)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := l.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	got := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], got)
	require.NoError(t, err)
	require.Equal(t, want.Value, got.Value)
}

func testTruncate(t *testing.T, l *Log) {
	want := &api.Record{Value: []byte("hello, world")}
	for i := 0; i < 3; i++ {
		_, err := l.Append(want)
		require.NoError(t, err)
	}
	err := l.Truncate(1)
	require.NoError(t, err)

	got, err := l.Read(0)
	require.Nil(t, got)
	require.Error(t, err)
}
