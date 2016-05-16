package curator

import (
	"github.com/cenkalti/backoff"
	"github.com/talbright/go-zookeeper/zk"

	"errors"
	"path"
	"sync"
	"time"
)

//ErrWatchStopped occurs when two calls to StopWatching are performed
var ErrWatchStopped = errors.New("the watch was stopped")

const defaultMaxRetryElapsedTime = 3 * time.Minute

/*
ChildWatch observes the children of a path in zookeeper for changes and
maintains a cached list of those nodes. Only changes in the children are
tracked, not changes in the data of the children. Some advantages over the
children watch in go-zookeeper include:

* tracks removals and additions for you in an easy to use map
* automatically restarts the watch when child watch events occur
* xmits events with nodes observed to have been added and/or removed
* returns data as proper Znode structures instead of a string
* stops a watch somewhat more cleanly than go-zookeeper
* can automatically retrieve child data (use carefully as a list of size n
would introduce n calls to retrieve data from zookeeper)

The children are stored as a map, with the Znode name as the key, and the Znode
as the value.

A ChildWatch is not re-usable and can only be started and stopped once. If
you need a to re-use a ChildWatch, create a new one with the same path. Due to
limitations in go-zookeeper a created ChildWatch will not free its goroutine
until a zookeeper connection or child watch event occurs.

*/
type ChildWatch struct {
	Path                string
	MaxRetryElapsedTime time.Duration
	RetrieveData        bool
	children            map[string]*Znode
	evntChn             chan Event
	client              *Client
	childWatch          <-chan zk.Event
	childrenLock        *sync.Mutex
	stopWatch           bool
	stopWatchLock       *sync.RWMutex
}

/*
NewChildWatch creates a watch on the provided path.

*/
func NewChildWatch(client *Client, path string) *ChildWatch {
	return &ChildWatch{
		Path:                path,
		MaxRetryElapsedTime: defaultMaxRetryElapsedTime,
		RetrieveData:        false,
		evntChn:             make(chan Event, 10),
		children:            make(map[string]*Znode),
		childrenLock:        &sync.Mutex{},
		stopWatchLock:       &sync.RWMutex{},
		client:              client,
	}
}

/*
WatchChildren starts watching the specified path and loads the intial set of
items.

*/
func (w *ChildWatch) WatchChildren() (chan Event, error) {
	w.stopWatchLock.Lock()
	defer w.stopWatchLock.Unlock()
	if w.stopWatch != false {
		return nil, ErrWatchStopped
	}
	if exists, _, err := w.client.Exists(w.Path); err != nil {
		return nil, err
	} else if !exists {
		return nil, ErrInvalidPath
	}
	go w.loop()
	return w.evntChn, nil
}

/*
StopWatching is a bit tricky because go-zookeeper doesn't cleanup child watches
nicely. In fact, its for this reason that a ChildWatch can only be started and
stopped once. When a ChildWatch is stopped the goroutine can't exit until a
watch event from zookeeper is received (again a limitation imposed by
go-zookeeper)

*/
func (w *ChildWatch) StopWatching() (err error) {
	w.stopWatchLock.RLock()
	defer w.stopWatchLock.RUnlock()
	if w.stopWatch != false {
		err = ErrWatchStopped
	} else {
		w.stopWatch = true
	}
	return
}

/*
GetChildren returns a copy of all child Znodes of Path. Note that after
you get the list it could immediately change. Using the channel returned by
WatchChildren is usually what you want.

*/
func (w *ChildWatch) GetChildren() map[string]Znode { return w.copyChildren() }

func (w *ChildWatch) loop() {

	var err error
	eventType := ChildrenWatchLoadedEvent
	w.getChildrenAndWatch(eventType)

	for {
		select {
		case event := <-w.childWatch:
			switch event.Type {
			case zk.EventNotWatching:
				if w.abortWatch() {
					return
				}
				eventType = ChildrenWatchLoadedEvent
				w.reset()
				err = w.getChildrenAndWatch(eventType)
			case zk.EventNodeChildrenChanged:
				if w.abortWatch() {
					return
				}
				eventType = ChildrenWatchChangedEvent
				err = w.getChildrenAndWatch(eventType)
			default:
				if w.abortWatch() {
					return
				}
				eventType = ChildrenWatchChangedEvent
				w.reset()
				err = w.getChildrenAndWatch(eventType)
			}
		}
		if err != nil {
			w.reset()
			w.emitEvent(eventType, nil, nil, err)
			return
		}
	}

}

func (w *ChildWatch) getChildrenAndWatch(event EventType) (err error) {
	var children []string
	retryCount := 0
	operation := func() error {
		retryCount++
		if children, _, w.childWatch, err = w.client.ChildrenW(w.Path); err == nil {
			add, rm := w.applyChanges(children)
			w.emitEvent(event, add, rm, nil)
		}
		return err
	}
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxElapsedTime = w.MaxRetryElapsedTime
	backoff.Retry(operation, expBackoff)
	return err
}

func (w *ChildWatch) emitEvent(eventType EventType, adds map[string]Znode, removals map[string]Znode, err error) {
	event := NewEvent(eventType, nil, err)
	event.Data["added"] = adds
	event.Data["removed"] = removals
	w.evntChn <- *event
}

func (w *ChildWatch) applyChanges(strnodes []string) (add map[string]Znode, rm map[string]Znode) {
	w.childrenLock.Lock()
	defer w.childrenLock.Unlock()

	//create comparison map from child nodes
	compare := make(map[string]*Znode)
	for _, v := range strnodes {
		znode := NewZnode(path.Join(w.Path, v))
		if w.RetrieveData {
			if data, stat, err := w.client.Get(path.Join(w.Path, v)); err == nil {
				znode.Data = data
				znode.Stat = stat
			}
		}
		compare[v] = znode
	}
	//find additions
	add = make(map[string]Znode)
	for k, v := range compare {
		if _, ok := w.children[k]; !ok {
			w.children[k] = v
			add[k] = *v.DeepCopy()
		}
	}
	//find removals
	rm = make(map[string]Znode)
	for k, v := range w.children {
		if _, ok := compare[k]; !ok {
			delete(w.children, k)
			rm[k] = *v.DeepCopy()
		}
	}
	return add, rm
}

func (w *ChildWatch) copyChildren() (newChildren map[string]Znode) {
	w.childrenLock.Lock()
	defer w.childrenLock.Unlock()
	if len(w.children) > 0 {
		newChildren = make(map[string]Znode)
		for k, v := range w.children {
			newChildren[k] = *v.DeepCopy()
		}
	}
	return newChildren
}

func (w *ChildWatch) reset() {
	w.childrenLock.Lock()
	defer w.childrenLock.Unlock()
	w.children = make(map[string]*Znode)
}

func (w *ChildWatch) abortWatch() bool {
	w.stopWatchLock.Lock()
	defer w.stopWatchLock.Unlock()
	return w.stopWatch == true
}
