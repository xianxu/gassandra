package gassandra

// Simple wrapper of cassandra behind the rpcx.Service, to take advantage of
// Supervisor, Replaceable and Cluster.
import (
	thrift "github.com/samuel/go-thrift"
	"log"
	"net"
	"net/rpc"
	"github.com/xianxu/rpcx"
	"time"
)

// Keyspace on a particular host
type Keyspace struct {
	Host     string
	Keyspace string
}

// Keyspace is a ServiceMaker
func (k Keyspace) Make() (name string, service rpcx.Service, err error) {
	name = "cassandra:" + k.Host
	conn, err := net.Dial("tcp", k.Host)
	if err != nil {
		log.Printf("Can't dial to %v on behalf of KeyspaceService", k.Host)
		return
	}

	client := thrift.NewClient(thrift.NewFramedReadWriteCloser(conn, 0), thrift.NewBinaryProtocol(true, false))

	req := &CassandraSetKeyspaceRequest{
		Keyspace: k.Keyspace,
	}
	res := &CassandraSetKeyspaceResponse{}
	err = client.Call("set_keyspace", req, res)
	switch {
	case res.Ire != nil:
		log.Printf("Can't set keyspace to %v on behalf of KeyspaceService", k.Keyspace)
		err = res.Ire
		return
	}

	service = KeyspaceService{client}
	return
}

type KeyspaceService struct {
	Client rpcx.RpcClient
}

// KeyspaceService is a Service
func (ks KeyspaceService) Serve(req interface{}, rsp interface{}, timeout time.Duration) (err error) {
	client := ks.Client
	var rpcReq *rpcx.RpcReq
	var ok bool
	if rpcReq, ok = req.(*rpcx.RpcReq); !ok {
		panic("wrong type passed")
	}
	if timeout > 0 {
		tick := time.After(timeout)

		call := client.Go(rpcReq.Fn, rpcReq.Args, rsp, make(chan *rpc.Call, 1))
		select {
		case c := <-call.Done:
			err = c.Error
		case <-tick:
			err = rpcx.TimeoutErr
		}
	} else {
		err = client.Call(rpcReq.Fn, rpcReq.Args, rsp)
	}
	return
}
