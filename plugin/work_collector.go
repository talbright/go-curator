package plugin

import (
	"path"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/davecgh/go-spew/spew"
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"
)

type WorkCollector struct {
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

func (p *WorkCollector) Name() string {
	return "WorkCollector"
}

func (p *WorkCollector) Accepts(eventType EventType) bool {
	return ConnectionEvent&eventType != 0
}

func (p *WorkCollector) Notify(event Event) {
	p.eventChn <- event
}

func (p *WorkCollector) OnLoad(curator *Curator) {
	p.workPath = path.Join(curator.Settings.RootPath, "members", p.ID, "work")
	p.client = curator.Client
	p.curator = curator
	p.mutex = &sync.RWMutex{}
	p.eventChn = make(chan Event, 10)
	p.stopChn = make(chan struct{})
	p.work = make(map[string]*Znode)
	p.WatchForWork()
}

func (p *WorkCollector) OnUnload() {
	p.StopWatching()
}

func (p *WorkCollector) WatchForWork() {
	go p.loop()
}

func (p *WorkCollector) StopWatching() {
	close(p.stopChn)
}

func (p *WorkCollector) Work() (work map[string]*Znode) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	work = make(map[string]*Znode)
	for k, v := range p.work {
		work[k] = v.DeepCopy()
	}
	return
}

func (p *WorkCollector) loop() {
	workWatchChn := make(chan Event)
	var err error
	entry := p.curator.LogEntry("work_collector")
	for {
		select {
		case event := <-p.eventChn:
			if p.workWatch == nil && event.IsConnectedEvent() {

				if err = p.client.CreatePath(p.workPath, zk.NoData, zk.WorldACLPermAll); err != nil && err != zk.ErrNodeExists {
					panic(err)
				}

				if err = p.client.WaitToExist(p.workPath, MaxWaitToExistTime); err != nil {
					panic(err)
				}

				p.workWatch = NewChildWatch(p.client, p.workPath)
				entry.WithField("path", p.workPath).Info("watching for work")
				if workWatchChn, err = p.workWatch.WatchChildren(); err != nil {
					panic(err)
				}
			}
		case event := <-workWatchChn:
			p.processWorkWatch(event)
		case <-p.stopChn:
			return
		}
	}
}

func (p *WorkCollector) processWorkWatch(event Event) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	entry := p.curator.LogEntry("work_collector")
	entry.WithFields(log.Fields{
		"event": event.Type,
		"spew":  spew.Sprintf("%#v", event),
	}).Debug("work watch change")
	added := make(map[string]Znode)
	removed := make(map[string]Znode)
	var ok bool
	if added, ok = event.Data["added"].(map[string]Znode); ok {
		entry.Debugf("adding %d nodes", len(added))
		for k, v := range added {
			fullPath := path.Join(p.workPath, k)
			p.work[fullPath] = &v
		}
	}
	if removed, ok = event.Data["removed"].(map[string]Znode); ok {
		entry.Debugf("removing %d nodes", len(removed))
		for k, _ := range removed {
			fullPath := path.Join(p.workPath, k)
			delete(p.work, fullPath)
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
