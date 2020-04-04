package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	CkID int64 // Clerk ID
	Seq  int64 // Sequence number of the RPC

}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string

	CkID int64
	Seq  int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
