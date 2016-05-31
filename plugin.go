package curator

type Plugin interface {
	Name() string
	Accepts(eventType EventType) bool
	Notify(event Event)
	OnLoad(curator *Curator)
	OnUnload()
}
