package curator

import (
	"github.com/Sirupsen/logrus"
	"github.com/davecgh/go-spew/spew"
	"github.com/talbright/go-zookeeper/zk"

	"fmt"
	"sync"
	"time"
)

const ChannelBufferSize = 20

func init() {
	spew.Config.Indent = "\t"
}

type Settings struct {
	ZkServers               []string
	ZkLogger                zk.Logger
	ZkSessionTimeout        time.Duration
	ZkWaitForSessionTimeout time.Duration
	ZkWaitForSession        bool
	RootPath                string
	Logger                  *logrus.Logger
	LogComponent            string
}

type Curator struct {
	Client   *Client
	Settings *Settings
	plugins  []Plugin
	mutex    *sync.RWMutex
	connChn  <-chan zk.Event
}

func NewCurator(client *Client, settings *Settings, plugins []Plugin) *Curator {
	if settings.Logger == nil {
		settings.Logger = logrus.StandardLogger()
	}
	curator := &Curator{
		Client:   client,
		Settings: settings,
		mutex:    &sync.RWMutex{},
		plugins:  make([]Plugin, 0),
	}
	for _, c := range plugins {
		curator.LoadPlugin(c)
	}
	return curator
}

func (c *Curator) Logger() *logrus.Logger {
	return c.Settings.Logger
}

func (c *Curator) LogEntry(suffix string) *logrus.Entry {
	return c.Settings.Logger.WithField("component", fmt.Sprintf("%s.%s", c.Settings.LogComponent, suffix))
}

func (c *Curator) LoadPlugin(plugin Plugin) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.Logger().WithField("component", c.Settings.LogComponent).Printf("loading plugin %s", plugin.Name())
	c.plugins = append(c.plugins, plugin)
	plugin.OnLoad(c)
}

func (c *Curator) UnloadPlugin(plugin Plugin) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
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

func (c *Curator) AllPlugins() (plugins []Plugin) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	plugins = make([]Plugin, 0)
	for _, v := range c.plugins {
		plugins = append(plugins, v)
	}
	return c.plugins
}

func (c *Curator) FindPlugin(f func(int, Plugin) bool) (index int, plugin Plugin) {
	for i, v := range c.AllPlugins() {
		if f(i, v) {
			return i, v
		}
	}
	return -1, nil
}

func (c *Curator) Start() (err error) {
	c.connChn, err = c.Client.Connect(c.Settings, zk.WithLogger(c.Settings.ZkLogger))
	if err == nil {
		go c.loop()
	}
	return
}

func (c *Curator) Stop() (err error) {
	//TODO
	//c.ClearPlugins()
	c.Client.Close()
	return
}

func (c *Curator) FireEvent(event Event) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	for _, plugin := range c.plugins {
		if plugin.Accepts(event.Type) {
			srcCopy := *(event.DeepCopy())
			plugin.Notify(srcCopy)
		}
	}
}

func (c *Curator) loop() {
	for event := range c.connChn {
		eventCopy := event
		c.FireEvent(Event{Type: ConnectionEvent, Source: &eventCopy})
	}
}
