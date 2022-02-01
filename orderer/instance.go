package orderer

import "github.ibm.com/mir-modular/manager"

type Instance interface {
	init(seg manager.Segment, orderer *Orderer)
	start()
	subscribeToBacklog()
	processSerializedMessages()
}
