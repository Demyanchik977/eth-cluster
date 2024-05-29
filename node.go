package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

type Node struct {
	height uint64 // latest height

	client *ethclient.Client

	fails    []int64 // list for fail time
	failsMtx *sync.Mutex

	quit chan bool
}

func NewNode(rpcUrl string) (*Node, error) {
	client, err := ethclient.Dial(rpcUrl)
	if err != nil {
		return nil, err
	}
	return &Node{
		client:   client,
		fails:    make([]int64, 0),
		failsMtx: new(sync.Mutex),
		quit:     make(chan bool, 1),
	}, nil
}

func (n *Node) heartbeat(interval int64) {
	duration := time.Second * time.Duration(interval)
	ticker := time.NewTicker(duration)
	for {
		select {
		case <-ticker.C:
			height, err := n.client.BlockNumber(context.Background())
			if err == nil {
				n.height = height
			}
		case <-n.quit:
			return
		}
	}
}

func (n *Node) FailCount() {
	n.failsMtx.Lock()
	defer n.failsMtx.Unlock()

	n.fails = append(n.fails, time.Now().Unix())
}

func (n *Node) Close() {
	close(n.quit)
}
