package curator

import (
	"github.com/davecgh/go-spew/spew"
)

type Plugin interface {
	Name() string
	Accepts(eventType EventType) bool
	Notify(event Event)
	OnLoad(curator *Curator, client *Client)
	OnUnload()
}

type EventSpewPlugin struct {
}

func (p *EventSpewPlugin) Name() string {
	return "EventSpew"
}
func (p *EventSpewPlugin) Accepts(eventType EventType) bool {
	return true
}
func (p *EventSpewPlugin) Notify(event Event) {
	spew.Printf("event: %#+v\n", event)
}
func (p *EventSpewPlugin) OnLoad(curator *Curator, client *Client) {
}
func (p *EventSpewPlugin) OnUnload() {
}
