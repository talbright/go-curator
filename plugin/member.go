package plugin

import (
	"path"
	"sync"

	"github.com/cenkalti/backoff"
	"github.com/davecgh/go-spew/spew"
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"
)

type Member struct {
	ID         string
	curator    *Curator
	client     *Client
	basePath   string
	memberPath string
	stopChn    chan struct{}
	eventChn   chan Event
	zkLock     *zk.Lock
	mutex      *sync.RWMutex
}

func (p *Member) Name() string {
	return "Member"
}

func (p *Member) Accepts(eventType EventType) bool {
	return ConnectionEvent&eventType != 0
}

func (p *Member) Notify(event Event) {
	p.eventChn <- event
}

func (p *Member) OnLoad(curator *Curator) {
	p.basePath = path.Join(curator.Settings.RootPath, "members")
	p.client = curator.Client
	p.curator = curator
	p.Register()
}

func (p *Member) OnUnload() {
	p.Unregister()
}

func (p *Member) MemberPath() string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.memberPath
}

func (p *Member) Register() {
	p.eventChn = make(chan Event, 10)
	p.stopChn = make(chan struct{})
	p.mutex = &sync.RWMutex{}
	go p.loop()
}

func (p *Member) Unregister() {
	close(p.stopChn)
	p.zkLock.Unlock()
	p.zkLock = nil
	p.setMemberPath("")
	p.curator.FireEvent(Event{Type: MemberEventUnregistered})
}

func (p *Member) setMemberPath(path string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.memberPath = path
}

func (p *Member) loop() {
	for {
		select {
		case event := <-p.eventChn:
			if event.IsConnectedEvent() {
				p.register(p.zkLock != nil)
			}
		case <-p.stopChn:
			return
		}
	}
}

func (p *Member) register(recoveryMode bool) {

	memberLockRoot := path.Join(p.basePath, p.ID, "lock")

	if err := p.client.CreatePath(memberLockRoot, zk.NoData, zk.WorldACLPermAll); err != nil && err != zk.ErrNodeExists {
		panic(err)
	}

	spew.Printf("Member: wait for path \"%s\" to exist\n", memberLockRoot)
	if err := p.client.WaitToExist(memberLockRoot, MaxWaitToExistTime); err != nil {
		panic(err)
	}

	if p.zkLock == nil {
		p.zkLock = zk.NewLock(p.client.Conn, memberLockRoot, zk.WorldACLPermAll)
	}

	operation := func() (err error) {
		//Try to unlock so we can re-aquire
		if recoveryMode {
			if err = p.zkLock.Unlock(); err != nil {
				if err == zk.ErrNoNode {
					p.zkLock = zk.NewLock(p.client.Conn, memberLockRoot, zk.WorldACLPermAll)
					err = nil
				} else if err == zk.ErrNotLocked {
					err = nil
				} else {
					return err
				}
			}
		}

		if path, err := p.zkLock.LockWithData(zk.NoData); err == nil {
			p.setMemberPath(path)
			p.curator.FireEvent(Event{Type: MemberEventRegistered, Node: NewZnode(path)})
		}

		return err
	}
	expBackoff := backoff.NewExponentialBackOff()
	backoff.Retry(operation, expBackoff)
}
