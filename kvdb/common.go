package kvdb

import uuid "github.com/satori/go.uuid"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	NoKeyValue     = ""
	RPCGet         = "KVServer.Get"
	RPCPutAppend   = "KVServer.PutAppend"
	OpPut          = "Put"
	OpAppend       = "Append"
	OpGet          = "Get"
	CommitTimeout  = "CommitTimeout"
)

type Err string

type PutAppendArgs struct {
	Key    string
	Value  string
	Op     string
	Id     uuid.UUID
	Serial uuid.UUID
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key    string
	Id     uuid.UUID
	Serial uuid.UUID
}

type GetReply struct {
	Err   Err
	Value string
}
