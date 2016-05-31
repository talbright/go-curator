package plugin

import (
	"path"
	"sync"

	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"
)

type Worker struct {
	ID        string
	curator   *Curator
	client    *Client
	workPath  string
	stopChn   chan struct{}
	eventChn  chan Event
	workWatch *ChildWatch
	mutex     *sync.RWMutex
	work      map[string]*Znode
}

func (p *Worker) Name() string {
	return "Worker"
}

func (p *Worker) Accepts(eventType EventType) bool {
	return ConnectionEvent&eventType != 0
}

func (p *Worker) Notify(event Event) {
	p.eventChn <- event
}

func (p *Worker) OnLoad(curator *Curator) {
	p.workPath = path.Join(curator.Settings.RootPath, "members", p.ID, "work")
	p.client = curator.Client
	p.curator = curator
	p.WatchForWork()
}

func (p *Worker) OnUnload() {
	p.StopWatching()
}

func (p *Worker) WatchForWork() {
	p.eventChn = make(chan Event, 10)
	p.stopChn = make(chan struct{})
	p.mutex = &sync.RWMutex{}
	p.work = make(map[string]*Znode)
	go p.loop()
}

func (p *Worker) StopWatching() {
	close(p.stopChn)
}

func (p *Worker) Work() (work map[string]*Znode) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	work = make(map[string]*Znode)
	for k, v := range p.work {
		work[k] = v.DeepCopy()
	}
	return
}

func (p *Worker) addWork(path string, node Znode) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.work[path] = &node
}

func (p *Worker) removeWork(path string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	delete(p.work, path)
}

func (p *Worker) loop() {
	workWatchChn := make(chan Event)
	var err error
	for {
		select {
		case event := <-p.eventChn:
			if p.workWatch == nil && IsHasSessionEvent(event) {
				if err = p.client.CreatePath(p.workPath, zk.NoData, zk.WorldACLPermAll); err != nil {
					panic(err)
				}
				p.workWatch = NewChildWatch(p.client, p.workPath)
				if workWatchChn, err = p.workWatch.WatchChildren(); err != nil {
					panic(err)
				}
			}
		case event := <-workWatchChn:
			p.processWorkWatchChangeset(event)
		case <-p.stopChn:
			return
		}
	}
}

func (p *Worker) processWorkWatchChangeset(event Event) {
	added := make(map[string]Znode)
	removed := make(map[string]Znode)
	var ok bool
	if added, ok = event.Data["added"].(map[string]Znode); ok {
		for k, v := range added {
			fullPath := path.Join(p.workPath, k)
			p.addWork(fullPath, v)
		}
	}
	if removed, ok = event.Data["removed"].(map[string]Znode); ok {
		for k, _ := range removed {
			fullPath := path.Join(p.workPath, k)
			p.removeWork(fullPath)
		}
	}

	var eventType EventType
	if event.Type == ChildrenWatchLoadedEvent {
		eventType = WorkerEventLoaded
	} else {
		eventType = WorkerEventChangeset
	}

	data := map[string]interface{}{
		"added":   added,
		"removed": removed,
		"path":    p.workPath,
	}

	p.curator.FireEvent(Event{Type: eventType, Data: data})
}
