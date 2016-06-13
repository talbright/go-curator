package curator

import (
	"github.com/talbright/go-zookeeper/zk"

	"errors"
	"fmt"
	"sync"
)

var ErrNodeNotInCache = errors.New("node not in cache")

/*
ChildCache provides a cached list, whose nodes are the children of a single
path within zookeeper. In other words, all the items in the cache would share
the same parent.

For example, supposing a zookeeper structure as follows:

/services/birdhouse/work
/services/birdhouse/work/123
/services/birdhouse/work/124
/services/birdhouse/work/125
/services/birdhouse/work/126

ChildCache provides operations on /services/birdhouse/work path to manipulate
its children: {123,124,125,126}.

The underlying list is stored as a map. Just like a filesystem structure in
unix, the children of a znode have unique names.

Some notes:

* Does not observe state changes in zk
* Nodes are persistant, not ephemeral
* Thread safe
* Does not require or use channels

*/
type ChildCache struct {
	parentPath    string
	client        *Client
	nodeCacheLock *sync.Mutex
	nodeCache     map[string]*Znode
}

func NewChildCache(client *Client, path string) *ChildCache {
	return &ChildCache{
		parentPath:    path,
		client:        client,
		nodeCache:     make(map[string]*Znode, 0),
		nodeCacheLock: &sync.Mutex{},
	}
}

/*
Shallow copy of cache that can be used for RO operations.
*/
func (l *ChildCache) Cache() (nodesMap map[string]Znode) {
	l.nodeCacheLock.Lock()
	defer l.nodeCacheLock.Unlock()
	nodesMap = make(map[string]Znode, 0)
	for _, v := range l.nodeCache {
		nodesMap[v.Name] = *v
	}
	return nodesMap
}

/*
List of children nodes for the path provided in the constructor.
*/
func (l *ChildCache) ToSlice() (nodes []Znode) {
	l.nodeCacheLock.Lock()
	defer l.nodeCacheLock.Unlock()
	nodes = make([]Znode, 0)
	for _, v := range l.nodeCache {
		nodes = append(nodes, *v)
	}
	return nodes
}

/*
Determine if a node exists by node name.
*/
func (l *ChildCache) Contains(name string) bool {
	l.nodeCacheLock.Lock()
	defer l.nodeCacheLock.Unlock()
	_, exists := l.nodeCache[name]
	return exists
}

/*
Retrieves a node from the cache if it exists, nil otherwise.
*/
func (l *ChildCache) Get(name string) (node *Znode) {
	l.nodeCacheLock.Lock()
	defer l.nodeCacheLock.Unlock()
	node, _ = l.nodeCache[name]
	return node
}

/*
Number of nodes in the cache.
*/
func (l *ChildCache) Size() int {
	l.nodeCacheLock.Lock()
	defer l.nodeCacheLock.Unlock()
	return len(l.nodeCache)
}

/*
Wipes local cache and reloads it directly from zk.
*/
func (l *ChildCache) LoadCache() (err error) {
	l.nodeCacheLock.Lock()
	defer l.nodeCacheLock.Unlock()

	l.nodeCache = make(map[string]*Znode, 0)
	if children, _, err := l.client.Children(l.parentPath); err == nil {
		for _, v := range children {
			unitPath := fmt.Sprintf("%s/%s", l.parentPath, v)
			if data, stat, err := l.client.Get(unitPath); err == nil {
				l.nodeCache[v] = &Znode{Name: v, Data: data, Stat: stat, Path: unitPath}
			} else {
				return err
			}
		}
	}
	return nil
}

/*
Add new nodes to underlying store, only if the node does not already exist in
cache. If the underlying node exists in Zk, its loaded into the cache (and not
considered an error). It's not considered an error to add a node that is
already in the cache.
*/
func (l *ChildCache) Add(nodes ...*Znode) (err error) {
	l.nodeCacheLock.Lock()
	defer l.nodeCacheLock.Unlock()
	for _, node := range nodes {
		if _, exists := l.nodeCache[node.Name]; !exists {
			nodePath := fmt.Sprintf("%s/%s", l.parentPath, node.Name)
			node.Path = nodePath
			exists, stat, err := l.client.Exists(nodePath)
			if err != nil {
				return err
			}
			if exists {
				node.Stat = stat
				l.nodeCache[node.Name] = node
			} else if _, err := l.client.Create(nodePath, node.Data, 0, zk.WorldACLPermAll); err == nil {
				if _, stat, err := l.client.Get(nodePath); err == nil {
					node.Stat = stat
				} else {
					return err
				}
				l.nodeCache[node.Name] = node
			} else {
				return err
			}
		}
	}
	return nil
}

/*
Delete nodes from underlying store, only if the node already exists in the
cache. If the node does not exist in the cache, an error is returned.
*/
func (l *ChildCache) Remove(nodes ...*Znode) (err error) {
	l.nodeCacheLock.Lock()
	defer l.nodeCacheLock.Unlock()
	for _, node := range nodes {
		if _, exists := l.nodeCache[node.Name]; exists {
			nodePath := fmt.Sprintf("%s/%s", l.parentPath, node.Name)
			if err := l.client.Delete(nodePath, -1); err == nil || err == zk.ErrNoNode {
				delete(l.nodeCache, node.Name)
			} else {
				return err
			}
		} else {
			return ErrNodeNotInCache
		}
	}
	return nil
}

/*
Clear all nodes from underlying store, but only if the node exists in the
cache.
*/
func (l *ChildCache) Clear() (err error) {
	l.nodeCacheLock.Lock()
	nodes := make([]*Znode, 0)
	for _, v := range l.nodeCache {
		nodes = append(nodes, v)
	}
	l.nodeCacheLock.Unlock()
	return l.Remove(nodes...)
}
