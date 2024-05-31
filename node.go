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

	lastTime int64
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

// heartbeat periodically updates the height of the node by querying the block number from the Ethereum client.
// It runs in a loop until the quit channel receives a signal.
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

// FailIncrease appends the current timestamp to the list of failures for the node.
func (n *Node) FailIncrease() {
	n.failsMtx.Lock()
	defer n.failsMtx.Unlock()

	n.fails = append(n.fails, time.Now().Unix())
}

// FailCount returns the number of failures that occurred within the specified duration.
// It locks the failsMtx mutex to ensure thread safety.
// The duration parameter specifies the time window in seconds.
// It counts the number of failures that occurred within the specified duration by comparing the current time with the timestamps of the failures.
// Returns the count of failures within the specified duration.
func (n *Node) FailCount(duration int64) int {
	n.failsMtx.Lock()
	defer n.failsMtx.Unlock()

	now := time.Now().Unix()

	count := 0
	for _, t := range n.fails {
		if now-t < duration {
			count++
		}
	}
	return count
}

func (n *Node) Close() {
	close(n.quit)
}
