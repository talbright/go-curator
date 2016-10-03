package plugin

import (
	"path"
	"sync"

	"github.com/davecgh/go-spew/spew"
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"
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
	spew.Printf("[curator] WorkLeader#AddWork: node %#v\n", node)
	workPath := path.Join(p.workPath, node.Name)
	_, err = p.client.Create(workPath, node.Data, zk.FlagPersistent, zk.WorldACLPermAll)
	return
}

// RemoveWork removes work from the master list, and is independent of
// leadership status. It is a convenience method that simply removes znodes
// from the appropriate path in zk.
func (p *WorkLeader) RemoveWork(node *Znode) (err error) {
	spew.Printf("[curator] WorkLeader#RemoveWork: node %#v\n", node)
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

func (p *WorkLeader) setLeader(leader bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.leader = leader
}

func (p *WorkLeader) startWorkLeader() {
	p.eventChn = make(chan Event, 10)
	p.stopChn = make(chan struct{})
	p.workerTracker = make(map[string]*Znode)
	p.mutex = &sync.RWMutex{}
	go p.loop()
}

func (p *WorkLeader) loop() {
	workWatchChn := make(chan Event)
	var err error
	for {
		select {
		case event := <-p.eventChn:
			spew.Printf("[curator] WorkLeader#loop: event %#v\n", event)
			if event.Type == LeaderEventElected {
				spew.Printf("[curator] WorkLeader#loop: becomming work leader!\n")
				if !p.IsLeader() {
					p.becomeLeader()
					if workWatchChn, err = p.workWatch.WatchChildren(); err != nil {
						panic(err)
					}
				}
			} else if event.Type == LeaderEventCandidate {
				spew.Printf("[curator] WorkLeader#loop: leader candidate\n")
			} else if event.Type == LeaderEventResigned {
				spew.Printf("[curator] WorkLeader#loop: resigning leadership!\n")
				if p.IsLeader() {
					p.resignLeader()
					workWatchChn = make(chan Event)
				}
				//Continuously track workers. An alternative would be to hold a reference to
				//the Discovery plugin, but that hasn't been flushed out yet. Curator holds
				//references to all plugins, however there are concurrency issues to consider.
			} else if event.Type == DiscoveryEventActive {
				spew.Printf("[curator] WorkLeader#loop: worker node is ACTIVE %s\n", event.Data["path"])
				p.addWorker(event.Data["path"].(string))
			} else if event.Type == DiscoveryEventInactive {
				spew.Printf("[curator] WorkLeader#loop: worker node is INACTIVE %s\n", event.Data["path"])
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
	p.setLeader(true)
	p.supervisor = NewWorkSupervisor(p.client, p.workPath)
	p.supervisor.Load()
	for _, v := range p.workerTracker {
		p.supervisor.AddWorker(v)
	}
	p.workWatch = NewChildWatch(p.client, p.workPath)

	if err := p.client.CreatePath(p.workPath, zk.NoData, zk.WorldACLPermAll); err != nil && err != zk.ErrNodeExists {
		panic(err)
	}

	if err := p.client.WaitToExist(p.workPath, MaxWaitToExistTime); err != nil {
		panic(err)
	}

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
	spew.Printf("[curator] WorkLeader#processWorkEvents: event %#v\n", event)
	if p.supervisor == nil {
		return
	}
	var fireEvent bool
	data := make(map[string]interface{})
	if added, ok := event.Data["added"].(map[string]Znode); ok {
		for _, v := range added {
			spew.Printf("[curator] WorkLeader#processWorkEvents: asking supervisor to add work %#v\n", v)
			p.supervisor.AddWork(&v)
		}
		if len(added) > 0 {
			data["work_added"] = added
			fireEvent = true
		}
	}
	if removed, ok := event.Data["removed"].(map[string]Znode); ok {
		for _, v := range removed {
			spew.Printf("[curator] WorkLeader#processWorkEvents: asking supervisor to remove work %#v\n", v)
			p.supervisor.RemoveWork(&v)
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
	workPath := path.Join(path.Dir(discoveryPath), "work")
	spew.Printf("[curator] WorkLeader#addWorker: path %s\n", workPath)
	workNode := NewZnode(workPath)
	data := map[string]interface{}{"worker_added": *workNode.DeepCopy()}
	p.curator.FireEvent(Event{Type: WorkLeaderChangeset, Data: data})
	p.workerTracker[workNode.Path] = workNode
	if p.IsLeader() {
		p.supervisor.AddWorker(workNode)
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
	workPath := path.Join(path.Dir(discoveryPath), "work")
	spew.Printf("[curator] WorkLeader#removeWorker: path %s\n", workPath)
	workNode := NewZnode(workPath)
	data := map[string]interface{}{"worker_removed": *workNode.DeepCopy()}
	p.curator.FireEvent(Event{Type: WorkLeaderChangeset, Data: data})
	delete(p.workerTracker, workPath)
	if p.IsLeader() {
		p.supervisor.RemoveWorker(workNode)
	}
}
