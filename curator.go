package curator

import (
	_ "github.com/davecgh/go-spew/spew"
	"github.com/talbright/go-zookeeper/zk"

	"sync"
	"time"
)

type Settings struct {
	ZkServers               []string
	ZkLogger                zk.Logger
	ZkSessionTimeout        time.Duration
	ZkWaitForSessionTimeout time.Duration
	ZkWaitForSession        bool
}

type Curator struct {
	client   *Client
	settings *Settings
	plugins  []Plugin
	mutex    *sync.Mutex
	connChn  <-chan zk.Event
}

func NewCurator(client *Client, settings *Settings, plugins []Plugin) *Curator {
	curator := &Curator{
		client:   client,
		settings: settings,
		mutex:    &sync.Mutex{},
		plugins:  make([]Plugin, 0),
	}
	for _, c := range plugins {
		curator.LoadPlugin(c)
	}
	return curator
}

func (c *Curator) LoadPlugin(plugin Plugin) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.plugins = append(c.plugins, plugin)
	plugin.OnLoad(c, c.client)
}

func (c *Curator) UnloadPlugin(plugin Plugin) {
	index := -1
	for i, p := range c.plugins {
		if p == plugin {
			index = i
			break
		}
	}
	if index >= 0 {
		c.plugins = append(c.plugins[:index], c.plugins[index+1:]...)
	}
}

func (c *Curator) ClearPlugins() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, p := range c.plugins {
		p.OnUnload()
	}
	c.plugins = make([]Plugin, 0)
}

func (c *Curator) AllPlugins() []Plugin {
	return c.plugins
}

func (c *Curator) Start() (err error) {
	c.connChn, err = c.client.Connect(c.settings, zk.WithLogger(c.settings.ZkLogger))
	if err == nil {
		go c.loop()
	}
	return
}

func (c *Curator) FireEvent(event Event) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, plugin := range c.plugins {
		if plugin.Accepts(event.Type) {
			plugin.Notify(event)
		}
	}
}

func (c *Curator) loop() {
	for event := range c.connChn {
		srcCopy := &event
		c.FireEvent(Event{Type: ConnectionEvent, Source: srcCopy})
	}
}
