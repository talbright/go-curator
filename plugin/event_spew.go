package plugin

import (
	"github.com/davecgh/go-spew/spew"
	. "github.com/talbright/go-curator"
	"log"
	"os"
)

type EventSpew struct {
	Log    *log.Logger
	client *Client
}

func (p *EventSpew) Name() string {
	return "EventSpew"
}

func (p *EventSpew) Accepts(eventType EventType) bool {
	return true
}

func (p *EventSpew) Notify(event Event) {
	p.Log.Printf(spew.Sprintf("%#x %#+v\n", p.client.SessionID(), event))
}

func (p *EventSpew) OnLoad(curator *Curator) {
	if p.Log == nil {
		p.Log = log.New(os.Stdout, "[ES] ", log.Ldate|log.Ltime)
	}
	p.client = curator.Client
}

func (p *EventSpew) OnUnload() {}
