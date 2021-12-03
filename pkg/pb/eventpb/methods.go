package eventpb

// TODO: Use the FollowUp function instead of direct access to the Event's Next field.

// FollowUp appends the Event next as a follow-up Event to this Event and returns the next Event (the argument).
// FollowUp can be called multiple times on the same Event and all the Events added this way
// will be guaranteed to be applied after this Event.
// The appended Events are, however, not guaranteed to be applied in any order relative to each other.
// Events (e.g., e1, e3, e3) that should be applied one after the other can be chained using chained calls to FollowUp:
// e1.FollowUp(e2).FollowUp(e3) guarantees application in this order.
// Note that the above expression evaluates to e3 and thus applying the whole expression will only evaluate e3.
// To process all three Events, e1 (and only e1) must be added to the Node's WorkItems.
func (e *Event) FollowUp(next *Event) *Event {
	e.Next = append(e.Next, next)
	return next
}

// FollowUps is like FollowUp, but appends multiple Events and does not return anything (and thus cannot be chained).
// e.FollowUps([]*Event{e1, e2, e3}) is equivalent to e.FollowUp(e1); e.FollowUp(e2); e.FollowUp(e3)
func (e *Event) FollowUps(next []*Event) {
	e.Next = append(e.Next, next...)
}
