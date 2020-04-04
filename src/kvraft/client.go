package raftkv

//import "time"
import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	id      int64
	seq     int64
	leader  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	ck.seq = 0
	ck.leader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.seq += 1
	args := GetArgs{
		Key:  key,
		CkID: ck.id,
		Seq:  ck.seq,
	}
	var reply GetReply
	ok := ck.sendGet(ck.leader, &args, &reply)
	if !ok || reply.WrongLeader {
		for {
			for i, _ := range ck.servers {
				reply = GetReply{}
				ok = ck.sendGet(i, &args, &reply)
				if ok && !reply.WrongLeader {
					ck.leader = i
					break
				}
			}
			if ok && !reply.WrongLeader {
				break
			}
		}
	}
	if reply.Err == OK {
		return reply.Value
	}
	return ""
}

func (ck *Clerk) sendGet(i int, args *GetArgs, reply *GetReply) bool {
	//time.Sleep(100 * time.Millisecond)
	DPrintf("raftkv %d <- Get %+v", i, args)
	ok := ck.servers[i].Call("KVServer.Get", args, reply)
	DPrintf("raftkv %d -> %v %+v", i, ok, reply)
	return ok
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seq += 1
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		CkID:  ck.id,
		Seq:   ck.seq,
	}
	var reply PutAppendReply
	ok := ck.sendPutAppend(ck.leader, &args, &reply)
	if ok && !reply.WrongLeader && reply.Err == OK {
		return
	}
	for {
		for i, _ := range ck.servers {
			reply = PutAppendReply{}
			ok = ck.sendPutAppend(i, &args, &reply)
			if ok && !reply.WrongLeader &&
				reply.Err == OK {
				ck.leader = i
				return
			}
		}
	}
}

func (ck *Clerk) sendPutAppend(
	i int, args *PutAppendArgs, reply *PutAppendReply) bool {
	//time.Sleep(100 * time.Millisecond)
	DPrintf("raftkv %d <- PutAppend %+v", i, args)
	ok := ck.servers[i].Call("KVServer.PutAppend", args, reply)
	DPrintf("raftkv %d -> %v %+v", i, ok, reply)
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
