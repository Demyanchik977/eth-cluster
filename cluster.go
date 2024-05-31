package cluster

import (
	"context"
	"errors"
	"log"
	"math/big"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var ErrNoData = errors.New("no data")

// Cluster represents a cluster of nodes.
type Cluster struct {
	duration int64       // duration specifies the duration of the cluster.
	nodes    []*Node     // nodes is a slice of nodes in the cluster.
	mtx      *sync.Mutex // mtx is a mutex used for synchronization.
	quit     chan bool   // quit is a channel used for signaling the cluster to stop.
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func NewCluster(rpcList []string, heartbeatInterval int64) (*Cluster, error) {
	nodes := make([]*Node, 0)
	for _, rpc := range rpcList {
		node, err := NewNode(rpc)
		if err != nil {
			log.Printf("node %s invalid", rpc)
		} else {
			nodes = append(nodes, node)
		}
	}
	cluster := &Cluster{
		nodes:    nodes,
		duration: heartbeatInterval,
		quit:     make(chan bool, 1),
		mtx:      new(sync.Mutex),
	}

	return cluster, nil
}

// Run starts the cluster and runs the heartbeat and sorting processes.
// It launches a goroutine for each node to perform heartbeat at the specified duration.
// It also starts a ticker to trigger the sorting process at the specified duration.
// The method will continue running until the quit channel receives a signal.
func (c *Cluster) Run() {
	for _, node := range c.nodes {
		go node.heartbeat(c.duration)
	}

	ticker := time.NewTicker(time.Second * time.Duration(c.duration))
	for {
		select {
		case <-ticker.C:
			c.sort()
		case <-c.quit:
			return
		}
	}
}

func (c *Cluster) Close() {
	for _, node := range c.nodes {
		node.Close()
	}
	close(c.quit)
}

// sort nodes by fail count and height
func (c *Cluster) sort() {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	sort.Slice(c.nodes, func(i, j int) bool {
		return c.nodes[i].FailCount(c.duration) < c.nodes[j].FailCount(c.duration)
	})

	sort.Slice(c.nodes, func(i, j int) bool {
		return c.nodes[i].height < c.nodes[j].height
	})

	// // Shuffle the first 50% of nodes
	// n := len(c.nodes) / 2
	// rand.Shuffle(n, func(i, j int) {
	// 	c.nodes[i], c.nodes[j] = c.nodes[j], c.nodes[i]
	// })
}

// ChainID retrieves the current chain ID for transaction replay protection.
func (c *Cluster) ChainId() (*big.Int, error) {
	for _, node := range c.nodes {
		chainId, err := node.client.ChainID(context.Background())
		if err != nil {
			node.FailIncrease()
		} else {
			return chainId, nil
		}
	}
	return nil, ErrNoData
}

// BlockByHash returns the given full block.
//
// Note that loading full blocks requires two requests. Use HeaderByHash
// if you don't need all transactions or uncle headers.
func (c *Cluster) BlockByHash(hash common.Hash) (*types.Block, error) {
	for _, node := range c.nodes {
		block, err := node.client.BlockByHash(context.Background(), hash)
		if err != nil {
			node.FailIncrease()
		} else {
			return block, nil
		}
	}
	return nil, ErrNoData
}

// BlockByNumber returns a block from the current canonical chain. If number is nil, the
// latest known block is returned.
//
// Note that loading full blocks requires two requests. Use HeaderByNumber
// if you don't need all transactions or uncle headers.
func (c *Cluster) BlockByNumber(num *big.Int) (*types.Block, error) {
	for _, node := range c.nodes {
		block, err := node.client.BlockByNumber(context.Background(), num)
		if err != nil {
			node.FailIncrease()
		} else {
			return block, nil
		}
	}
	return nil, ErrNoData
}

// BlockNumber returns the most recent block number
func (c *Cluster) BlockNumber() (uint64, error) {
	for _, node := range c.nodes {
		height, err := node.client.BlockNumber(context.Background())
		if err != nil {
			node.FailIncrease()
		} else {
			return height, nil
		}
	}
	return 0, ErrNoData
}

// HeaderByHash returns the block header with the given hash.
func (c *Cluster) HeaderByHash(hash common.Hash) (*types.Header, error) {
	for _, node := range c.nodes {
		header, err := node.client.HeaderByHash(context.Background(), hash)
		if err != nil {
			node.FailIncrease()
		} else {
			return header, nil
		}
	}
	return nil, ErrNoData
}

// HeaderByNumber returns a block header from the current canonical chain. If number is
// nil, the latest known header is returned.
func (c *Cluster) HeaderByNumber(number *big.Int) (*types.Header, error) {
	for _, node := range c.nodes {
		header, err := node.client.HeaderByNumber(context.Background(), number)
		if err != nil {
			node.FailIncrease()
		} else {
			return header, nil
		}
	}
	return nil, ErrNoData
}

// TransactionByHash returns the transaction with the given hash.
func (c *Cluster) TransactionByHash(hash common.Hash) (tx *types.Transaction, isPending bool, err error) {
	for _, node := range c.nodes {
		tx, isPending, err = node.client.TransactionByHash(context.Background(), hash)
		if err != nil {
			node.FailIncrease()
		} else {
			return
		}
	}
	return nil, false, ErrNoData
}

// TransactionCount returns the total number of transactions in the given block.
func (c *Cluster) TransactionCount(blockHash common.Hash) (uint, error) {
	for _, node := range c.nodes {
		num, err := node.client.TransactionCount(context.Background(), blockHash)
		if err != nil {
			node.FailIncrease()
		} else {
			return num, nil
		}
	}
	return 0, ErrNoData
}

// TransactionInBlock returns a single transaction at index in the given block.
func (c *Cluster) TransactionInBlock(blockHash common.Hash, index uint) (*types.Transaction, error) {
	for _, node := range c.nodes {
		tx, err := node.client.TransactionInBlock(context.Background(), blockHash, index)
		if err != nil {
			node.FailIncrease()
		} else {
			return tx, nil
		}
	}
	return nil, ErrNoData
}

// TransactionReceipt returns the receipt of a transaction by transaction hash.
// Note that the receipt is not available for pending transactions.
func (c *Cluster) TransactionReceipt(txHash common.Hash) (*types.Receipt, error) {
	for _, node := range c.nodes {
		receipt, err := node.client.TransactionReceipt(context.Background(), txHash)
		if err != nil {
			node.FailIncrease()
		} else {
			return receipt, nil
		}
	}
	return nil, ErrNoData
}

// State Access

// NetworkID returns the network ID for this client.
func (c *Cluster) NetworkID() (*big.Int, error) {
	for _, node := range c.nodes {
		id, err := node.client.NetworkID(context.Background())
		if err != nil {
			node.FailIncrease()
		} else {
			return id, nil
		}
	}
	return nil, ErrNoData
}

// BalanceAt returns the wei balance of the given account.
// The block number can be nil, in which case the balance is taken from the latest known block.
func (c *Cluster) BalanceAt(account common.Address, blockNumber *big.Int) (*big.Int, error) {
	for _, node := range c.nodes {
		balance, err := node.client.BalanceAt(context.Background(), account, blockNumber)
		if err != nil {
			node.FailIncrease()
		} else {
			return balance, nil
		}
	}
	return nil, ErrNoData
}

// StorageAt returns the value of key in the contract storage of the given account.
// The block number can be nil, in which case the value is taken from the latest known block.
func (c *Cluster) StorageAt(account common.Address, key common.Hash, blockNumber *big.Int) ([]byte, error) {
	for _, node := range c.nodes {
		storage, err := node.client.StorageAt(context.Background(), account, key, blockNumber)
		if err != nil {
			node.FailIncrease()
		} else {
			return storage, nil
		}
	}
	return nil, ErrNoData
}

// CodeAt returns the contract code of the given account.
// The block number can be nil, in which case the code is taken from the latest known block.
func (c *Cluster) CodeAt(account common.Address, blockNumber *big.Int) ([]byte, error) {
	for _, node := range c.nodes {
		code, err := node.client.CodeAt(context.Background(), account, blockNumber)
		if err != nil {
			node.FailIncrease()
		} else {
			return code, nil
		}
	}
	return nil, ErrNoData
}

// NonceAt returns the account nonce of the given account.
// The block number can be nil, in which case the nonce is taken from the latest known block.
func (c *Cluster) NonceAt(account common.Address, blockNumber *big.Int) (uint64, error) {
	for _, node := range c.nodes {
		nonce, err := node.client.NonceAt(context.Background(), account, blockNumber)
		if err != nil {
			node.FailIncrease()
		} else {
			return nonce, nil
		}
	}
	return 0, ErrNoData
}

// FilterLogs executes a filter query.
func (c *Cluster) FilterLogs(start, end uint64, contract common.Address) ([]types.Log, error) {
	query := setQuery(start, end, contract)
	for _, node := range c.nodes {
		logs, err := node.client.FilterLogs(context.Background(), query)
		if err != nil {
			node.FailIncrease()
		} else {
			return logs, nil
		}
	}
	return nil, ErrNoData
}

func setQuery(start, end uint64, contract common.Address) ethereum.FilterQuery {
	return ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(start),
		ToBlock:   new(big.Int).SetUint64(end),
		Addresses: []common.Address{contract},
	}
}
