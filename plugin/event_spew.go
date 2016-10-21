package plugin

import (
	"github.com/Sirupsen/logrus"
	"github.com/davecgh/go-spew/spew"
	. "github.com/talbright/go-curator"
)

type EventSpew struct {
	Log    *logrus.Entry
	client *Client
}

func (p *EventSpew) Name() string {
	return "EventSpew"
}

func (p *EventSpew) Accepts(eventType EventType) bool {
	return true
}

func (p *EventSpew) Notify(event Event) {
	p.Log.Debugf(spew.Sprintf("%#x %#+v\n", p.client.SessionID(), event))
}

func (p *EventSpew) OnLoad(curator *Curator) {
	if p.Log == nil {
		p.Log = curator.LogEntry("spew")
	}
	p.client = curator.Client
}

func (p *EventSpew) OnUnload() {}
