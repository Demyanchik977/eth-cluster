package cluster

import (
	"context"
	"errors"
	"log"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var ErrNoData = errors.New("no data")

type Cluster struct {
	nodes []*Node
}

func NewCluster(rpcList []string) (*Cluster, error) {
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
		nodes: nodes,
	}
	return cluster, nil
}

func (c *Cluster) BlockNumber() (uint64, error) {
	for _, node := range c.nodes {
		height, err := node.client.BlockNumber(context.Background())
		if err == nil {
			return height, nil
		}
	}
	return 0, ErrNoData
}

func (c *Cluster) FilterLogs(start, end uint64, contract common.Address) ([]types.Log, error) {
	query := setQuery(start, end, contract)
	for _, node := range c.nodes {
		logs, err := node.client.FilterLogs(context.Background(), query)
		if err == nil {
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
