package pbservice

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrRepeatedRPC = "ErrRepeatedRPC"
	Debug = false
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op string
	Src string
	Sn int

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
	Forwarded string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
	Forwarded bool
}


// Your RPC definitions here.
type SyncArgs struct {
	DB map[string]string
	LastElem map[string]string
}

type SyncReply struct{
	
}

func Dprint(message ...interface{}){
	if Debug{
		fmt.Println(message)
	}
}