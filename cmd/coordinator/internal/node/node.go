package node

type Node struct {
	id  int
	url string
}

func MakeNode(id int, url string) Node {
	return Node{id, url}
}

type VNode struct {
	nodeId int
	hash   uint32
}
