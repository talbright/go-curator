package curator

import (
	"errors"
	"fmt"
	"github.com/talbright/go-zookeeper/zk"
	"strings"
	"time"
)

//TODO create default logger and assign (logrus)
// func init() {
// 	zk.DefaultLogger = ??
// }

//ZkNoData is a shortcut to representing "no data" when making common client calls
var ZkNoData = []byte{0}

//ZkWorldACL is a shortcut to representing zk.Permall when making common client calls
var ZkWorldACL = zk.WorldACL(zk.PermAll)

//ErrConnectionTimedOut occurs when initial connection attempt to zk fails
var ErrConnectionTimedOut = errors.New("connection to zookeeper timed out")

//ErrInvalidPath occurs when the provided path in zk is malformed
var ErrInvalidPath = errors.New("provided path is invalid")

//ZkConnectionSettings provide connection options for zk
type ZkConnectionSettings struct {
	Servers           []string
	SessionTimeout    time.Duration
	ConnectionTimeout time.Duration
	WaitForSession    bool
}

/*
Client connects to and interacts with zk.
*/
type Client struct {
	*zk.Conn
	EventChannel <-chan zk.Event
}

/*
NewClient creates a client that can interact with zk
*/
func NewClient() *Client {
	return &Client{}
}

/*
Connect creates a connection to zookeeper for the client
*/
func (c *Client) Connect(servers []string, sessionTimeout time.Duration) (err error) {
	c.Conn, c.EventChannel, err = zk.Connect(servers, sessionTimeout)
	return err
}

/*
ConnectWithSettings creates a connection to zookeeper for the client. If
WaitForSession is set to true, the caller will be blocked until the client has
established a session to Zookeeper.
*/
func (c *Client) ConnectWithSettings(settings *ZkConnectionSettings) (err error) {
	c.Conn, c.EventChannel, err = zk.Connect(settings.Servers, settings.SessionTimeout)
	if settings.WaitForSession && err == nil {
		timeout := make(chan bool, 1)
		if settings.ConnectionTimeout > 0 {
			go func() {
				time.Sleep(settings.ConnectionTimeout)
				timeout <- true
			}()
		}
		for {
			select {
			case <-timeout:
				return ErrConnectionTimedOut
			case event := <-c.EventChannel:
				if event.Type == zk.EventSession {
					switch event.State {
					case zk.StateHasSession:
						return nil
					}
				}
			}
		}
	}
	return err
}

/*
CreatePath will create the full path in zookeeper (emulates 'mkdir -p'). Each
node will be assigned the same data and acl permissions. Only non-ephemeral
nodes can have children.

The path parameter must begin with '/'
*/
func (c *Client) CreatePath(path string, data []byte, acl []zk.ACL) error {
	if !strings.HasPrefix(path, "/") {
		return ErrInvalidPath
	}
	segments := strings.Split(path, "/")
	segments = segments[1:len(segments)]
	var slice []string
	for i := range segments {
		slice = append(slice, segments[i])
		segment := fmt.Sprintf("/%s", strings.Join(slice, "/"))
		exists, _, err := c.Exists(segment)
		if err != nil {
			return err
		}
		if exists != true {
			_, err := c.Create(segment, data, 0, acl)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
