package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
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
	CkID  int64
	Seq   int64
	Op    string
	Key   string
	Value string
}

type Result struct {
	Seq   int64
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	isKilled bool

	maxraftstate int // snapshot if log grows this big

	resultMap map[int64]Result
	m         map[string]string
}

func (kv *KVServer) dup(ckID int64, seq int64) (Result, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	result, ok := kv.resultMap[ckID]
	if !ok || seq > result.Seq {
		return result, false
	}
	return result, true
}

func (kv *KVServer) exec(op Op) (Result, bool) {
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return Result{}, false
	}
	for {
		term1, isLeader := kv.rf.GetState()
		if term1 != term || !isLeader {
			return Result{}, false
		}
		kv.mu.Lock()
		result, ok := kv.resultMap[op.CkID]
		if !ok || result.Seq < op.Seq {
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		kv.mu.Unlock()
		return result, true
	}

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("KVS %d <-- Get %+v", kv.me, args)
	defer func() { DPrintf("KVS %d --> Get %+v", kv.me, reply) }()
	result, executed := kv.dup(args.CkID, args.Seq)
	if executed {
		reply.Err = result.Err
		reply.Value = result.Value
		return
	}

	op := Op{
		CkID: args.CkID,
		Seq:  args.Seq,
		Op:   "Get",
		Key:  args.Key,
	}

	result, ok := kv.exec(op)
	if ok {
		reply.Err = result.Err
		reply.Value = result.Value
	} else {
		reply.WrongLeader = true
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("KVS %d <-- PutAppend %+v", kv.me, args)
	defer func() { DPrintf("KVS %d --> PutAppend %+v", kv.me, reply) }()
	result, executed := kv.dup(args.CkID, args.Seq)
	if executed {
		reply.Err = result.Err
		return
	}

	op := Op{
		CkID:  args.CkID,
		Seq:   args.Seq,
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,
	}

	result, ok := kv.exec(op)
	if ok {
		reply.Err = result.Err
	} else {
		reply.WrongLeader = true
	}

}

func (kv *KVServer) apply() {
	for msg := range kv.applyCh {
		DPrintf("KVS %d: Apply %+v", kv.me, msg)
		//index := msg.CommandIndex
		op := msg.Command.(Op)

		kv.mu.Lock()
		if kv.isKilled {
			return
		}

		result, ok := kv.resultMap[op.CkID]
		if !ok || result.Seq < op.Seq {
			result = kv.doApply(op)
			kv.resultMap[op.CkID] = result
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) doApply(op Op) Result {
	var result Result
	switch op.Op {
	case "Get":
		value, ok := kv.m[op.Key]
		if !ok {
			result.Err = ErrNoKey
		} else {
			result.Err = OK
			result.Value = value
		}
	case "Put":
		kv.m[op.Key] = op.Value
		result.Err = OK
	case "Append":
		value, ok := kv.m[op.Key]
		if !ok {
			kv.m[op.Key] = op.Value
		} else {
			kv.m[op.Key] = value + op.Value
		}
		result.Err = OK
	}
	result.Seq = op.Seq
	return result
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.isKilled = true
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.isKilled = false

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.resultMap = make(map[int64]Result)
	kv.m = make(map[string]string)

	go kv.apply()

	return kv
}
