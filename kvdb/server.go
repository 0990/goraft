package kvdb

import (
	"fmt"
	"github.com/0990/goraft/labgob"
	"github.com/0990/goraft/raft"
	"github.com/0990/goraft/rpcutil"
	uuid "github.com/satori/go.uuid"
	"log"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type   string
	Key    string
	Value  string
	Serial uuid.UUID
}

type CommonReply struct {
	Err    Err
	Key    string
	Value  string
	Serial *uuid.UUID
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32
	maxRaftState int

	data          map[string]string
	commonReplies []*CommonReply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	op := &Op{
		Type:   OpGet,
		Key:    args.Key,
		Value:  NoKeyValue,
		Serial: args.Serial,
	}
	reply.Err = ErrWrongLeader
	idx, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		DPrintf("%v 对于 %v 的 Get 请求 {Key=%v Serial=%v} 处理结果为 该服务器不是领导者",
			kv.me, args.Id, args.Key, args.Serial)
		return nil
	}

	fmt.Printf("%v 等待对 %v 的 Get 请求 {Key=%v Serial=%v} 的提交，应提交索引为 %v\n",
		kv.me, args.Id, args.Key, args.Serial, idx)

	commonReply := &CommonReply{}
	find := kv.findReply(op, idx, commonReply)
	if find == OK {
		reply.Value = commonReply.Value
		reply.Err = commonReply.Err
	}

	fmt.Printf("%v 对于 %v 的 Get 请求 {Key=%v Serial=%v} 处理结果为 %v, len(Value)=%v\n",
		kv.me, args.Id, args.Key, args.Serial, reply.Err, len(reply.Value))
	return nil
}

func (kv *KVServer) findReply(op *Op, idx int, reply *CommonReply) string {
	t0 := time.Now()
	for time.Since(t0).Seconds() < 2 {
		kv.mu.Lock()
		if len(kv.commonReplies) > idx {
			if op.Serial == *kv.commonReplies[idx].Serial {
				reply1 := kv.commonReplies[idx]
				reply.Err = reply1.Err
				reply.Key = reply1.Key
				reply.Value = reply1.Value
				kv.mu.Unlock()
				return OK
			} else {
				kv.mu.Unlock()
				return CommitTimeout
			}
		}
		kv.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	return CommitTimeout
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	op := &Op{
		Type:   args.Op,
		Key:    args.Key,
		Value:  args.Value,
		Serial: args.Serial,
	}
	reply.Err = ErrWrongLeader
	idx, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		DPrintf("%v 对于 %v 的 %v 请求 {Key=%v Serial=%v Value='%v'} 处理结果为 该服务器不是领导者",
			kv.me, args.Id, args.Op, args.Key, args.Serial, args.Value)
		return nil
	}
	fmt.Printf("%v 等待对 %v 的 %v 请求 {Key=%v Serial=%v Value='%v'} 的提交，应提交索引为 %v\n",
		kv.me, args.Id, args.Op, args.Key, args.Serial, args.Value, idx)

	commonReply := &CommonReply{}
	find := kv.findReply(op, idx, commonReply)
	if find == OK {
		reply.Err = commonReply.Err
	}
	fmt.Printf("%v 对于 %v 的 %v 请求 {Key=%v Serial=%v Value='%v'} 处理结果为 %v\n",
		kv.me, args.Id, args.Op, args.Key, args.Serial, args.Value, reply.Err)
	return nil
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) IsKilled() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) opHandler() {
	for {
		if kv.IsKilled() {
			return
		}

		applyMsg := <-kv.applyCh
		op := applyMsg.Command.(Op)
		reply := &CommonReply{
			Key:    op.Key,
			Value:  op.Value,
			Serial: &op.Serial,
		}

		switch op.Type {
		case OpPut:
			kv.data[op.Key] = op.Value
			reply.Err = OK
		case OpAppend:
			if _, has := kv.data[op.Key]; has {
				kv.data[op.Key] += op.Value
			} else {
				kv.data[op.Key] = op.Value
			}
			reply.Err = OK
		case OpGet:
			if value, ok := kv.data[op.Key]; ok {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
		default:
			fmt.Sprintf("%v: Get 命令中 op.Type 的值 %v 错误，附 op 的值 {Key=%v Value=%v}",
				kv.me, op.Type, op.Key, op.Value)
		}

		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			kv.commonReplies = append(kv.commonReplies, reply)
		}()
	}
}

func StartKVServer(servers []*rpcutil.ClientEnd, me int, persister *raft.Persister, maxRafeState int, rpcServer *rpc.Server) (*KVServer, error) {
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxRafeState
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.MakeRaft(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.commonReplies = make([]*CommonReply, 1)

	if err := rpcServer.Register(kv.rf); err != nil {
		return nil, err
	}

	go kv.opHandler()
	return kv, nil
}
