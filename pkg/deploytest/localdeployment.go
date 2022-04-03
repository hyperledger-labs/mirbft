package deploytest

import (
	"fmt"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

func LocalAddresses(nodeIDs []t.NodeID, basePort int) map[t.NodeID]string {
	addrs := make(map[t.NodeID]string)
	for _, i := range nodeIDs {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+int(i))
	}
	return addrs
}
