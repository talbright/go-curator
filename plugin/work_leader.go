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
	workerTracker map[string]*Znode
	mutex         *sync.RWMutex
}

func (p *WorkLeader) Name() string {
	return "WorkLeader"
}

func (p *WorkLeader) Accepts(eventType EventType) bool {
	mask := MemberEventRegistered | MemberEventUnregistered | LeaderEventElected | LeaderEventResigned | ConnectionEvent
	return mask&eventType != 0
}

func (p *WorkLeader) Notify(event Event) {
	p.eventChn <- event
}

func (p *WorkLeader) OnLoad(curator *Curator) {
	p.workPath = path.Join(curator.Settings.RootPath, "work")
	p.client = curator.Client
	p.curator = curator
	p.StartLeadingWork()
}

func (p *WorkLeader) OnUnload() {
	close(p.stopChn)
}

func (p *WorkLeader) StartLeadingWork() {
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
			if p.workWatch == nil && event.IsConnectedEvent() {
				if err = p.client.CreatePath(p.workPath, zk.NoData, zk.WorldACLPermAll); err != nil && err != zk.ErrNodeExists {
					panic(err)
				}
				p.workWatch = NewChildWatch(p.client, p.workPath)
				if workWatchChn, err = p.workWatch.WatchChildren(); err != nil {
					panic(err)
				}
			}
			if p.workWatch != nil {
				p.processExternalEvents(event)
			}
		case event := <-workWatchChn:
			p.processWorkWatchChangeset(event)
		case <-p.stopChn:
			return
		}
	}
}

//Adds and removes work for the supervisor via child watch. The supervisor
//uses ChildCache so work isn't lost between leadership changes.
func (p *WorkLeader) processWorkWatchChangeset(event Event) {
	spew.Println("processWorkWatchChangeset")
	if p.supervisor == nil {
		return
	}
	if added, ok := event.Data["added"].(map[string]Znode); ok {
		for _, v := range added {
			spew.Printf("processWorkWatchChangeset: add work %s\n", v.Path)
			p.supervisor.AddWork(&v)
		}
	}
	if removed, ok := event.Data["removed"].(map[string]Znode); ok {
		for _, v := range removed {
			spew.Printf("processWorkWatchChangeset: remove work %s\n", v.Path)
			p.supervisor.RemoveWork(&v)
		}
	}
}

//Adds and removes workers to supervisor and initializes supervisor
func (p *WorkLeader) processExternalEvents(event Event) {
	spew.Println("processExternalEvents")
	switch event.Type {
	case LeaderEventElected:
		if p.supervisor != nil {
			spew.Println("LeaderEventElected: already leader")
			return
		}
		spew.Println("LeaderEventElected: create supervisor")
		spew.Printf("LeaderEventElected: work path is %s\n", p.workPath)
		p.supervisor = NewWorkSupervisor(p.client, p.workPath)
		p.supervisor.Load()
		for _, v := range p.workerTracker {
			p.supervisor.AddWorker(v)
		}
	case LeaderEventResigned:
		spew.Println("LeaderEventResigned: nil supervisor")
		p.supervisor = nil
	case MemberEventUnregistered:
		workPath := path.Join(path.Dir(path.Dir(event.Node.Path)), "work")
		spew.Printf("MemberEventUnregistered: remove worker from tracker %s\n", workPath)
		delete(p.workerTracker, workPath)
		if p.supervisor != nil {
			spew.Printf("MemberEventUnregistered: remove worker from supervisor %s\n", event.Node.Path)
			p.supervisor.RemoveWorker(event.Node)
		}
	case MemberEventRegistered:
		workPath := path.Join(path.Dir(path.Dir(event.Node.Path)), "work")
		workNode := NewZnode(workPath)
		spew.Printf("MemberEventRegistered: add worker to tracker %s\n", workNode.Path)
		p.workerTracker[workNode.Path] = workNode
		if p.supervisor != nil {
			spew.Printf("MemberEventRegistered: add worker to supervisor %s\n", workNode.Path)
			p.supervisor.AddWorker(workNode)
		}
	}
}
