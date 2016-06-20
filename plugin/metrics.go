package plugin

import (
	_ "github.com/davecgh/go-spew/spew"
	"github.com/rcrowley/go-metrics"
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"
)

type Metrics struct {
	Registry metrics.Registry
	client   *Client
}

func (p *Metrics) Name() string {
	return "Metrics"
}

func (p *Metrics) Accepts(eventType EventType) bool {
	return true
}

func (p *Metrics) Notify(event Event) {
	p.eventCounter().Inc(1)
	switch event.Type {
	case ConnectionEvent:
		p.metricsForConnection(event)
	case LeaderEventElected:
		fallthrough
	case LeaderEventCandidate:
		fallthrough
	case LeaderEventResigned:
		p.metricsForLeadership(event)
	case MemberEventRegistered:
		fallthrough
	case MemberEventUnregistered:
		p.metricsForMembership(event)
	case DiscoveryEventActive:
		fallthrough
	case DiscoveryEventInactive:
		p.metricsForDiscovery(event)
	case WorkLeaderChangeset:
		fallthrough
	case WorkLeaderActive:
		fallthrough
	case WorkLeaderInactive:
		p.metricsForWorkLeader(event)
	}
}

func (p *Metrics) OnLoad(curator *Curator) {
	if p.Registry == nil {
		p.Registry = metrics.NewPrefixedRegistry("curator.")
	}
	p.client = curator.Client
	p.initMetrics()
}

func (p *Metrics) OnUnload() {}

func (p *Metrics) metricsForWorkLeader(event Event) {
	switch event.Type {
	case WorkLeaderChangeset:
		if event.Data != nil {
			if _, ok := event.Data["worker_added"]; ok {
				p.workLeaderWorkerCount().Inc(1)
			}
			if _, ok := event.Data["worker_removed"]; ok {
				p.workLeaderWorkerCount().Dec(1)
			}
			if _, ok := event.Data["work_added"]; ok {
				if added, ok := event.Data["work_added"].(map[string]Znode); ok {
					p.workLeaderWorkCount().Inc(int64(len(added)))
				}
			}
			if _, ok := event.Data["work_removed"]; ok {
				if removed, ok := event.Data["work_removed"].(map[string]Znode); ok {
					p.workLeaderWorkCount().Dec(int64(len(removed)))
				}
			}
		}
	case WorkLeaderActive:
		p.workLeaderActiveGauge().Update(1)
	case WorkLeaderInactive:
		p.workLeaderActiveGauge().Update(0)
	}
}

func (p *Metrics) metricsForDiscovery(event Event) {
	switch event.Type {
	case DiscoveryEventActive:
		p.discoveryCounter().Inc(1)
	case DiscoveryEventInactive:
		if p.discoveryCounter().Count() > 0 {
			p.discoveryCounter().Dec(1)
		}
	}
}

func (p *Metrics) metricsForMembership(event Event) {
	switch event.Type {
	case MemberEventRegistered:
		p.registeredGauge().Update(1)
	case MemberEventUnregistered:
		p.registeredGauge().Update(0)
	}
}

func (p *Metrics) metricsForLeadership(event Event) {
	switch event.Type {
	case LeaderEventElected:
		p.leaderGauge().Update(1)
	case LeaderEventCandidate:
		fallthrough
	case LeaderEventResigned:
		p.leaderGauge().Update(0)
	}
}

func (p *Metrics) metricsForConnection(event Event) {
	if event.Source != nil {
		switch event.Source.State {
		case zk.StateHasSession:
			p.sessionGauge().Update(1)
		case zk.StateConnecting:
			p.sessionGauge().Update(0)
			p.connectingCounter().Inc(1)
		case zk.StateUnknown:
			break
		default:
			p.sessionGauge().Update(0)
		}
	}
}

func (p *Metrics) initMetrics() {
	p.workLeaderActiveGauge().Update(0)
	p.workLeaderWorkerCount().Clear()
	p.workLeaderWorkCount().Clear()
	p.discoveryCounter().Clear()
	p.eventCounter().Clear()
	p.registeredGauge().Update(0)
	p.leaderGauge().Update(0)
	p.sessionGauge().Update(0)
	p.connectingCounter().Clear()
}

func (p *Metrics) workLeaderActiveGauge() metrics.Gauge {
	return metrics.GetOrRegisterGauge("work_leader.active", p.Registry)
}

func (p *Metrics) workLeaderWorkerCount() metrics.Counter {
	return metrics.GetOrRegisterCounter("work_leader.workers", p.Registry)
}

func (p *Metrics) workLeaderWorkCount() metrics.Counter {
	return metrics.GetOrRegisterCounter("work_leader.work", p.Registry)
}

func (p *Metrics) discoveryCounter() metrics.Counter {
	return metrics.GetOrRegisterCounter("discovery.discovered", p.Registry)
}

func (p *Metrics) eventCounter() metrics.Counter {
	return metrics.GetOrRegisterCounter("events", p.Registry)
}

func (p *Metrics) registeredGauge() metrics.Gauge {
	return metrics.GetOrRegisterGauge("member.registered", p.Registry)
}

func (p *Metrics) leaderGauge() metrics.Gauge {
	return metrics.GetOrRegisterGauge("leader.elected", p.Registry)
}

func (p *Metrics) sessionGauge() metrics.Gauge {
	return metrics.GetOrRegisterGauge("connection.session", p.Registry)
}

func (p *Metrics) connectingCounter() metrics.Counter {
	return metrics.GetOrRegisterCounter("connection.attempts", p.Registry)
}
