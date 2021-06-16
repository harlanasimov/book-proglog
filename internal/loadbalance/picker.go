package loadbalance

import (
	"strings"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*Picker)(nil)

type Picker struct {
	mu        sync.RWMutex
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64
	logger    *zap.Logger
}

func (p *Picker) Build(info base.PickerBuildInfo) balancer.Picker {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.logger == nil {
		p.logger = zap.L().Named("picker")
	}
	var followers []balancer.SubConn
	for sc, scinfo := range info.ReadySCs {
		isLeader := scinfo.Address.Attributes.Value("is_leader").(bool)
		//p.logger.Debug("Picker.Build:", zap.Any("Addr", scinfo.Address.Addr), zap.Any("Attributes", scinfo.Address.Attributes), zap.Bool("isLeader", isLeader))
		if isLeader {
			p.leader = sc
			continue
		}

		followers = append(followers, sc)
	}
	p.followers = followers
	//p.logger.Debug("Picker.Build:", zap.Any("leader", p.leader), zap.Any("followers", p.followers))
	return p
}

func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	//p.logger.Debug("Picker.Pick:", zap.Any("info", info))
	var result balancer.PickResult
	if len(p.followers) == 0 {
		result.SubConn = p.leader
	} else if strings.Contains(info.FullMethodName, "Produce") {
		result.SubConn = p.leader
	} else if strings.Contains(info.FullMethodName, "Consume") {
		result.SubConn = p.nextFollower()
	}
	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}
	return result, nil
}

func (p *Picker) nextFollower() balancer.SubConn {
	cur := atomic.AddUint64(&p.current, 1)
	len := uint64(len(p.followers))
	idx := int(cur % len)
	return p.followers[idx]
}

func init() {
	balancer.Register(base.NewBalancerBuilder(Name, &Picker{}, base.Config{}))
}
