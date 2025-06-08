package server

import (
	"errors"
	"fmt"
	"github.com/0990/goraft/kvdb"
	"github.com/0990/goraft/raft"
	"github.com/0990/goraft/rpcutil"
	"log"
	"net"
	"net/rpc"
)

func Run(cfgPath string) error {
	serverCfg, err := parseCfg(cfgPath)
	if err != nil {
		return err
	}
	i := len(serverCfg.Servers)
	if i%2 == 0 {
		return errors.New("服务器数量必须为单数")
	}

	clientEnds := getClientEnds(serverCfg)

	persister := raft.MakePersister()

	rpcServer := rpc.NewServer()

	kvur, err := kvdb.StartKVServer(clientEnds, serverCfg.Me, persister, i, rpcServer)
	if err != nil {
		return err
	}

	if err := rpcServer.Register(kvur); err != nil {
		return err
	}

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", serverCfg.Servers[serverCfg.Me].Port))
	if err != nil {
		return err
	}
	log.Println("Listen", serverCfg.Servers[serverCfg.Me].Port)
	rpcServer.Accept(l)
	return nil
}

func getClientEnds(serverCfg Config) []*rpcutil.ClientEnd {
	clientEnds := make([]*rpcutil.ClientEnd, 0)
	for i, end := range serverCfg.Servers {
		address := fmt.Sprintf("%s:%d", end.Ip, end.Port)
		var client *rpc.Client
		if i == serverCfg.Me {
			client = nil
		} else {
			client = rpcutil.TryConnect(address)
		}

		tmpClientEnd := &rpcutil.ClientEnd{
			Addr:   address,
			Client: client,
		}
		clientEnds = append(clientEnds, tmpClientEnd)
	}
	return clientEnds
}
