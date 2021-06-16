package log

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	api "github.com/wenyuangui/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{
		config: config,
	}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

func (l *DistributedLog) setupRaft(dataDir string) error {
	fsm := &fsm{log: l.log}

	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	logConfig := l.config
	logConfig.Segment.InitialOffset = 1
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}

	ratain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft"), ratain, os.Stderr)
	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(l.config.Raft.StreamLayer, maxPool, timeout, os.Stderr)

	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if logConfig.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if logConfig.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if logConfig.Raft.CommitTimeout != 0 {
		config.CommitTimeout = logConfig.Raft.CommitTimeout
	}

	l.raft, err = raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return err
	}

	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}

	return err
}

func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(AppendRequestType, &api.ProduceRequest{Record: record})
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	future := l.raft.Apply(buf.Bytes(), 10*time.Second)
	if future.Error() != nil {
		return nil, err
	}
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

type RequestType uint8

const (
	AppendRequestType = 0
)

func (f *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

func (f *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return nil
	}
	off, err := f.log.Append(req.Record)
	if err != nil {
		return nil
	}
	return &api.ProduceResponse{Offset: off}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{Reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	Reader io.Reader
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.Reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

func (f *fsm) Restore(r io.ReadCloser) error {
	var recBuf bytes.Buffer
	sizeBuf := make([]byte, lenWidth)
	for i := 0; ; i++ {
		// size
		_, err := io.ReadFull(r, sizeBuf)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := enc.Uint64(sizeBuf)
		// record
		if _, err := io.CopyN(&recBuf, r, int64(size)); err != nil {
			return err
		}
		record := &api.Record{}
		if err := proto.Unmarshal(recBuf.Bytes(), record); err != nil {
			return err
		}
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err := f.log.Append(record); err != nil {
			return err
		}
		recBuf.Reset()
	}
	return nil
}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}

	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(ln net.Listener, serverTLSConfig *tls.Config, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{ln: ln, serverTLSConfig: serverTLSConfig, peerTLSConfig: peerTLSConfig}
}

const RaftRPC = 1

func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, nil
}

func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if bytes.Compare(b, []byte{byte(RaftRPC)}) != 0 {
		return nil, errors.New("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		conn = tls.Server(conn, s.serverTLSConfig)
	}
	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}

func (l *DistributedLog) Join(id, addr string) error {
	configFutrue := l.raft.GetConfiguration()
	if err := configFutrue.Error(); err != nil {
		return nil
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFutrue.Configuration().Servers {
		if srv.ID == serverID && srv.Address == serverAddr {
			return nil
		}

		removeFuture := l.raft.RemoveServer(serverID, 0, 0)
		if err := removeFuture.Error(); err != nil {
			return err
		}
	}
	addFutrue := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFutrue.Error(); err != nil {
		return err
	}
	return nil
}

func (l *DistributedLog) Leave(id string) error {
	serverID := raft.ServerID(id)
	removeFuture := l.raft.RemoveServer(serverID, 0, 0)
	if err := removeFuture.Error(); err != nil {
		return err
	}
	return nil
}

func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutC := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-timeoutC:
			return errors.New("timed out")
		case <-ticker.C:
			if ld := l.raft.Leader(); ld != "" {
				return nil
			}
		}
	}
}

func (l *DistributedLog) Close() error {
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return l.log.Close()
}

func (l *DistributedLog) GetServers() ([]*api.Server, error) {
	future := l.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var servers []*api.Server
	for _, server := range future.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:       string(server.ID),
			RpcAddr:  string(server.Address),
			IsLeader: l.raft.Leader() == server.Address})
	}
	return servers, nil
}
