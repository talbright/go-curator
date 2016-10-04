package plugin

import (
	"path"
	"sync"

	"github.com/cenkalti/backoff"
	_ "github.com/davecgh/go-spew/spew"
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"
)

type Leader struct {
	curator   *Curator
	client    *Client
	basePath  string
	lockPath  string
	stopChn   chan struct{}
	eventChn  chan Event
	zkLock    *zk.Lock
	mutex     *sync.RWMutex
	leader    bool
	Signature []byte
}

func (p *Leader) Name() string {
	return "Leader"
}

func (p *Leader) Accepts(eventType EventType) bool {
	return ConnectionEvent&eventType != 0
}

func (p *Leader) Notify(event Event) {
	p.eventChn <- event
}

func (p *Leader) OnLoad(curator *Curator) {
	p.basePath = path.Join(curator.Settings.RootPath, "leader")
	p.client = curator.Client
	p.curator = curator
	p.RunForElection()
}

func (p *Leader) OnUnload() {
	p.Resign()
}

func (p *Leader) RunForElection() {
	p.eventChn = make(chan Event, 10)
	p.stopChn = make(chan struct{})
	p.mutex = &sync.RWMutex{}
	go p.loop()
}

func (p *Leader) Resign() {
	close(p.stopChn)
	p.zkLock.Unlock()
	p.zkLock = nil
	p.setLeader(false)
	p.setLockPath("")
	p.curator.FireEvent(Event{Type: LeaderEventResigned})
}

func (p *Leader) IsLeader() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.leader
}

func (p *Leader) LockPath() string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.lockPath
}

func (p *Leader) setLeader(leader bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.leader = leader
}

func (p *Leader) setLockPath(path string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.lockPath = path
}

func (p *Leader) loop() {
	for {
		select {
		case event := <-p.eventChn:
			if event.IsConnectedEvent() {
				p.campaign(p.zkLock != nil)
			}
		case <-p.stopChn:
			return
		}
	}
}

func (p *Leader) campaign(recoveryMode bool) {
	p.setLeader(false)
	p.curator.FireEvent(Event{Type: LeaderEventCandidate})
	if p.zkLock == nil {
		p.zkLock = zk.NewLock(p.client.Conn, p.basePath, zk.WorldACLPermAll)
	}

	operation := func() (err error) {
		//Try to unlock so we can re-aquire
		if recoveryMode {
			if err = p.zkLock.Unlock(); err != nil {
				if err == zk.ErrNoNode {
					p.zkLock = zk.NewLock(p.client.Conn, p.basePath, zk.WorldACLPermAll)
					err = nil
				} else if err == zk.ErrNotLocked {
					err = nil
				} else {
					return err
				}
			}
		}

		if path, err := p.zkLock.LockWithData(p.Signature); err == nil {
			p.setLockPath(path)
			p.curator.FireEvent(Event{Type: LeaderEventElected, Node: NewZnode(path)})
		}

		return err
	}
	expBackoff := backoff.NewExponentialBackOff()
	backoff.Retry(operation, expBackoff)
}
