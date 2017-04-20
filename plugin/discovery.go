package plugin

import (
	"path"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/cenkalti/backoff"
	"github.com/davecgh/go-spew/spew"
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"
)

type MembershipMeta struct {
	stopChn chan struct{}
	znode   *Znode
	active  bool
}

type Discovery struct {
	curator    *Curator
	client     *Client
	memberPath string
	stopChn    chan struct{}
	eventChn   chan Event
	mutex      *sync.RWMutex
	members    map[string]*MembershipMeta
	rootWatch  *ChildWatch
}

func (p *Discovery) Name() string {
	return "Discovery"
}

func (p *Discovery) Accepts(eventType EventType) bool {
	return ConnectionEvent&eventType != 0
}

func (p *Discovery) Notify(event Event) {
	p.eventChn <- event
}

func (p *Discovery) OnLoad(curator *Curator) {
	p.memberPath = path.Join(curator.Settings.RootPath, "members")
	p.client = curator.Client
	p.curator = curator
	p.StartDiscovery()
}

func (p *Discovery) OnUnload() {
	p.StopDiscovery()
}

func (p *Discovery) StartDiscovery() {
	p.eventChn = make(chan Event, ChannelBufferSize)
	p.stopChn = make(chan struct{})
	p.mutex = &sync.RWMutex{}
	p.members = make(map[string]*MembershipMeta)
	go p.loop()
}

func (p *Discovery) StopDiscovery() {
	close(p.stopChn)
	p.clearMembers()
}

func (p *Discovery) Members() map[string]*Znode {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	members := make(map[string]*Znode, 0)
	for k, v := range p.members {
		members[k] = v.znode
	}
	return members
}

func (p *Discovery) getMember(path string) (meta *MembershipMeta) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	meta, _ = p.members[path]
	return meta
}

func (p *Discovery) hasMember(path string) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	_, exists := p.members[path]
	return exists
}

func (p *Discovery) clearMembers() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for _, v := range p.members {
		close(v.stopChn)
	}
	p.members = make(map[string]*MembershipMeta, 0)
}

func (p *Discovery) mergeMember(path string, meta *MembershipMeta) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	entry := p.curator.LogEntry("discovery").WithFields(log.Fields{
		"path": path,
		"meta": spew.Sprintf("%#v", meta),
	})
	entry.WithField("spew", p.members).Debug("current members")
	if updateMeta, exists := p.members[path]; exists {
		if updateMeta.active == meta.active {
			entry.Warn("tracking: member already exists and active")
			return
		} else {
			entry.Info("tracking: member already exists and became active")
			updateMeta.active = meta.active
			updateMeta.znode = meta.znode
		}
	} else {
		entry.Info("tracking: new member")
		p.members[path] = meta
	}
	var eventType EventType
	if p.members[path].active {
		eventType = DiscoveryEventActive
	} else {
		eventType = DiscoveryEventInactive
	}
	data := map[string]interface{}{"path": path}
	p.curator.FireEvent(Event{Type: eventType, Node: meta.znode, Data: data})
}

func (p *Discovery) loop() {
	rootWatchChn := make(chan Event)
	var err error
	entry := p.curator.LogEntry("discovery")
	for {
		select {
		case event := <-p.eventChn:
			if p.rootWatch == nil && event.IsConnectedEvent() {

				if err := p.client.CreatePath(p.memberPath, zk.NoData, zk.WorldACLPermAll); err != nil && err != zk.ErrNodeExists {
					panic(err)
				}

				if err = p.client.WaitToExist(p.memberPath, MaxWaitToExistTime); err != nil {
					panic(err)
				}

				p.rootWatch = NewChildWatch(p.client, p.memberPath)
				entry.WithField("path", p.memberPath).Info("watching for members")
				if rootWatchChn, err = p.rootWatch.WatchChildren(); err != nil {
					panic(err)
				}
			}
		case event := <-rootWatchChn:
			p.processRootWatchChangeset(event)
		case <-p.stopChn:
			return
		}
	}
}

func (p *Discovery) processRootWatchChangeset(event Event) {
	entry := p.curator.LogEntry("discovery")
	entry.WithFields(log.Fields{
		"event": event.Type,
		"spew":  spew.Sprintf("%#v", event),
	}).Debug("root membership change")
	if added, yes := event.Data["added"].(map[string]Znode); yes {
		for k, _ := range added {
			fullPath := path.Join(p.memberPath, k)
			if !p.hasMember(fullPath) {
				p.startDescendentWatch(path.Join(fullPath, "lock"))
			}
		}
	}
}

func (p *Discovery) startDescendentWatch(watchPath string) {
	entry := p.curator.LogEntry("discovery").WithField("path", watchPath)
	entry.Info("starting descendent watch")
	go func() {

		retryCount := 0
		var scopedChn <-chan zk.Event

		//closure to set watch and update membership based on results from child watch
		operation := func() (err error) {
			retryCount++
			var children []string
			if children, _, scopedChn, err = p.client.ChildrenW(watchPath); err == nil {
				meta := &MembershipMeta{
					stopChn: make(chan struct{}),
				}
				entry.WithField("spew", children).Debug("descendent watch found children")
				if len(children) > 0 {
					meta.znode = NewZnode(path.Join(watchPath, children[0]))
					meta.active = true
				}
				p.mergeMember(watchPath, meta)
			}
			return err
		}

		for {
			expBackoff := backoff.NewExponentialBackOff()
			backoff.Retry(operation, expBackoff)
			retryCount = 0
			select {
			case <-scopedChn:
				//No action required
			case <-p.stopChn:
				p.client.CancelWatch(scopedChn)
				return
			}
		}

	}()

}
