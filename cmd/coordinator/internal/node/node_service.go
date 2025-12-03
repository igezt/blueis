package node

import (
	"fmt"
	"hash/fnv"
	"sort"
)

type NodeService struct {
	nodesPerWeight int
	nodes          map[int]Node
	vnodes         []VNode
	latestNodeId   int
}

func fnv32(data []byte) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(data)
	return h.Sum32()
}

func MakeNodeService(nodesPerWeight int) NodeService {
	return NodeService{nodesPerWeight, make(map[int]Node), make([]VNode, 0), 0}
}

func (nodeService *NodeService) AddNode(url string, weight int) {
	numVNodes := weight * nodeService.nodesPerWeight
	id := nodeService.latestNodeId
	nodeService.latestNodeId += 1
	for i := range numVNodes {
		key := fmt.Sprintf("%d-%d", id, i)
		hash := fnv32([]byte(key))
		nodeService.vnodes = append(nodeService.vnodes, VNode{id, hash})
	}

	sort.Slice(nodeService.vnodes, func(i, j int) bool {
		return nodeService.vnodes[i].hash < nodeService.vnodes[j].hash
	})
	nodeService.nodes[id] = MakeNode(id, url)
}

func (nodeService *NodeService) RemoveNode(id int) {
	out := make([]VNode, 0)
	for _, vn := range nodeService.vnodes {
		if id != vn.nodeId {
			out = append(out, vn)
		}
	}
	nodeService.vnodes = out
	delete(nodeService.nodes, id)
}

func (nodeService *NodeService) FindNode(hash uint32) Node {
	// First vnode with hash >= given hash
	idx := sort.Search(len(nodeService.vnodes), func(i int) bool {
		return nodeService.vnodes[i].hash >= hash
	})

	// Wrap around if necessary
	if idx == len(nodeService.vnodes) {
		idx = 0
	}

	vn := nodeService.vnodes[idx]
	node := nodeService.nodes[vn.nodeId]
	return node
}
