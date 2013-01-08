Introduction
============

Gassandra is a simple wrapper of Cassandra. It wraps Cassandra as a rpcx.Service (for a given
Keyspace) with interface:

    Serve(req interface{}, rsp interface{}, timeout time.Duration) error

Where req is a rpcx.RpcReq { Fn string, Args interface{} }
