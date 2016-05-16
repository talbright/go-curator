package curator

import (
	"github.com/talbright/go-zookeeper/zk"

	"path"
)

/*
Znode is an in memory only representation of a Zookeeper node.

The current implementaiton does not provide any synchronization primitives or
CRUD operations.
*/
type Znode struct {
	Name string
	Path string
	Data []byte
	Stat *zk.Stat
}

/*
NewZnode creates a in-memory representation of a znode given the provided
path.
*/
func NewZnode(path string) *Znode {
	node := &Znode{Path: path}
	node.Name = node.Basename()
	return node
}

/*
Parent returns the name of this Znodes parent
*/
func (n Znode) Parent() string {
	return path.Dir(n.Path)
}

/*
Basename returns the leaf name of this Znode
*/
func (n Znode) Basename() string {
	return path.Base(n.Path)
}

/*
DeepCopy creates a deepcopy of n
*/
func (n Znode) DeepCopy() (zcopy *Znode) {
	zcopy = NewZnode(n.Path)
	if n.Data != nil {
		zcopy.Data = make([]byte, len(n.Data))
		copy(zcopy.Data, n.Data)
	}
	if n.Stat != nil {
		statCopy := *n.Stat
		zcopy.Stat = &statCopy
	}
	return
}
