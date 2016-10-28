package plugin

import (
	log "github.com/Sirupsen/logrus"
	"github.com/davecgh/go-spew/spew"
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"

	"fmt"
	"path"
	"sync"
)

type WorkLeader struct {
	curator       *Curator
	client        *Client
	workPath      string
	stopChn       chan struct{}
	eventChn      chan Event
	workWatch     *ChildWatch
	supervisor    *WorkSupervisor
	workerTracker map[string]*Znode //otherwise we need to hold a reference to the discovery plugin...
	mutex         *sync.RWMutex
	leader        bool
}

func (p *WorkLeader) Name() string {
	return "WorkLeader"
}

func (p *WorkLeader) Accepts(eventType EventType) bool {
	mask := DiscoveryEventInactive | DiscoveryEventActive | LeaderEventElected | LeaderEventCandidate | LeaderEventResigned | ConnectionEvent
	return mask&eventType != 0
}

func (p *WorkLeader) Notify(event Event) {
	p.eventChn <- event
}

func (p *WorkLeader) OnLoad(curator *Curator) {
	p.workPath = path.Join(curator.Settings.RootPath, "work")
	p.client = curator.Client
	p.curator = curator
	p.startWorkLeader()
}

func (p *WorkLeader) OnUnload() {
	close(p.stopChn)
}

// AddWork adds work to the master list, and is independent of leadership
// status. It is a convenience method that simply writes znodes to the
// appropriate path in zk.
func (p *WorkLeader) AddWork(node *Znode) (err error) {
	workPath := path.Join(p.workPath, node.Name)
	_, err = p.client.Create(workPath, node.Data, zk.FlagPersistent, zk.WorldACLPermAll)
	return
}

// RemoveWork removes work from the master list, and is independent of
// leadership status. It is a convenience method that simply removes znodes
// from the appropriate path in zk.
func (p *WorkLeader) RemoveWork(node *Znode) (err error) {
	workPath := path.Join(p.workPath, node.Name)
	var version int32
	if node.Stat != nil {
		version = node.Stat.Version
	} else {
		version = -1
	}
	err = p.client.Delete(workPath, version)
	return
}

func (p *WorkLeader) IsLeader() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.leader == true
}

func (p *WorkLeader) entry() *log.Entry {
	return p.curator.LogEntry("work_leader").WithField("myfield", "x").WithField("leader", fmt.Sprintf("%t", p.IsLeader()))
}

func (p *WorkLeader) setLeader(leader bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.leader = leader
}

func (p *WorkLeader) startWorkLeader() {
	p.eventChn = make(chan Event, ChannelBufferSize)
	p.stopChn = make(chan struct{})
	p.workerTracker = make(map[string]*Znode)
	p.mutex = &sync.RWMutex{}
	go p.loop()
}

func (p *WorkLeader) loop() {
	workWatchChn := make(chan Event)
	var err error
	entry := p.entry()

	for {
		select {
		case event := <-p.eventChn:
			if event.Type == LeaderEventElected {
				entry.Info("work leader: elected")
				if !p.IsLeader() {
					p.becomeLeader()
					if workWatchChn, err = p.workWatch.WatchChildren(); err != nil {
						panic(err)
					}
				} else {
					entry.Warn("work leader: already elected")
				}
			} else if event.Type == LeaderEventCandidate {
				entry.Info("work leader: candidate")
			} else if event.Type == LeaderEventResigned {
				entry.Info("work leader: resigned")
				if p.IsLeader() {
					p.resignLeader()
					workWatchChn = make(chan Event)
				}
				//Continuously track workers. An alternative would be to hold a reference to
				//the Discovery plugin, but that hasn't been flushed out yet. Curator holds
				//references to all plugins, however there are concurrency issues to consider.
			} else if event.Type == DiscoveryEventActive {
				entry.WithField("path", event.Data["path"]).Info("worker discovery: worker activated")
				p.addWorker(event.Data["path"].(string))
			} else if event.Type == DiscoveryEventInactive {
				entry.WithField("path", event.Data["path"]).Info("worker discovery: worker deactivated")
				p.removeWorker(event.Data["path"].(string))
			}
		case event := <-workWatchChn:
			p.processWorkEvents(event)
		case <-p.stopChn:
			return
		}
	}
}

func (p *WorkLeader) becomeLeader() {
	entry := p.entry()

	if err := p.client.CreatePath(p.workPath, zk.NoData, zk.WorldACLPermAll); err != nil && err != zk.ErrNodeExists {
		panic(err)
	}

	if err := p.client.WaitToExist(p.workPath, MaxWaitToExistTime); err != nil {
		panic(err)
	}

	p.supervisor = NewWorkSupervisor(p.client, p.workPath)
	p.supervisor.Logger = p.curator.Logger()
	p.supervisor.LogComponent = fmt.Sprintf("%s.%s", p.curator.Settings.LogComponent, "work_supervisor")
	p.supervisor.Load()
	entry.WithField("workers", p.workerTracker).Debug("adding workers to supervisor")
	for _, v := range p.workerTracker {
		if err := p.supervisor.AddWorker(v); err != nil {
			entry.WithError(err).WithField("worker", spew.Sprintf("%#v", v)).Error("unable to add worker")
		} else {
			entry.WithField("worker", v.Spew()).Debug("added worker to supervisor")
		}
	}

	entry.WithField("path", p.workPath).Info("setting watch for work assignments")

	p.setLeader(true)

	p.workWatch = NewChildWatch(p.client, p.workPath)

	p.curator.FireEvent(Event{Type: WorkLeaderActive})
}

func (p *WorkLeader) resignLeader() {
	p.setLeader(false)
	p.workWatch = nil
	p.supervisor = nil
	p.curator.FireEvent(Event{Type: WorkLeaderInactive})
}

//Adds and removes work for the supervisor (via child watch.) The supervisor
//uses ChildCache so work isn't lost between leadership changes.
func (p *WorkLeader) processWorkEvents(event Event) {
	entry := p.entry()
	entry.WithFields(log.Fields{
		"event": event.Type,
		"spew":  spew.Sprintf("%#v", event),
	}).Debug("work watch change")
	if p.supervisor == nil {
		entry.Warn("supervisor was not set")
		return
	}
	var fireEvent bool
	data := make(map[string]interface{})
	if added, ok := event.Data["added"].(map[string]Znode); ok {
		entry.WithField("count", len(added)).Debug("asking supervisor to add work")
		for _, v := range added {
			p.supervisor.AddWork(v.DeepCopy())
		}
		if len(added) > 0 {
			data["work_added"] = added
			fireEvent = true
		}
	}
	if removed, ok := event.Data["removed"].(map[string]Znode); ok {
		entry.WithField("count", len(removed)).Debug("asking supervisor to remove work")
		for _, v := range removed {
			if err := p.supervisor.RemoveWork(v.DeepCopy()); err != nil {
				entry.WithError(err).WithField("work", spew.Sprintf("%#v", v)).Error("unable to remove work")
			}
		}
		if len(removed) > 0 {
			data["work_removed"] = removed
			fireEvent = true
		}
	}
	if fireEvent {
		p.curator.FireEvent(Event{Type: WorkLeaderChangeset, Data: data})
	}
}

/*
addWorker tracks workers (ultimately via Discovery plugin events). The
WorkSupervisor expects a Znode with a path to the zk directory that will hold
the work. With the way paths are setup in Curator, and events are xmitted via
the Discover pluigin, the input path to this method should look something like:

/services/{service}/{node}/lock

This path is munged so it is suitable and in the format expected by the
Supervisor:

/services/{service}/{node}/work

*/
func (p *WorkLeader) addWorker(discoveryPath string) {
	entry := p.entry()
	workPath := path.Join(path.Dir(discoveryPath), "work")
	entry.WithField("path", workPath).Debug("adding worker")
	workNode := NewZnode(workPath)
	data := map[string]interface{}{"worker_added": *workNode.DeepCopy()}
	p.curator.FireEvent(Event{Type: WorkLeaderChangeset, Data: data})
	p.workerTracker[workNode.Path] = workNode
	entry.WithField("spew", p.workerTracker).Debug("worker tracker")
	if p.IsLeader() {
		if err := p.supervisor.AddWorker(workNode); err != nil {
			entry.WithError(err).WithField("worker", spew.Sprintf("%#v", workNode)).Error("unable to add worker")
		}
	}
}

/*
removeWorker tracks workers (ultimately via Discovery plugin events). The
WorkSupervisor expects a Znode with a path to the zk directory that will hold
the work. With the way paths are setup in Curator, and events are xmitted via
the Discover pluigin, the input path to this method should look something like:

/services/{service}/{node}/lock

This path is munged so it is suitable and in the format expected by the
Supervisor:

/services/{service}/{node}/work

*/
func (p *WorkLeader) removeWorker(discoveryPath string) {
	entry := p.entry()
	workPath := path.Join(path.Dir(discoveryPath), "work")
	entry.WithField("path", workPath).Debug("removing worker")
	workNode := NewZnode(workPath)
	data := map[string]interface{}{"worker_removed": *workNode.DeepCopy()}
	p.curator.FireEvent(Event{Type: WorkLeaderChangeset, Data: data})
	delete(p.workerTracker, workPath)
	entry.WithField("spew", p.workerTracker).Debug("worker tracker")
	if p.IsLeader() {
		if err := p.supervisor.RemoveWorker(workNode); err != nil {
			entry.WithError(err).WithField("worker", spew.Sprintf("%#v", workNode)).Error("unable to remove worker")
		}
	}
}
