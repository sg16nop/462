package pbservice

import "../viewservice"
import "net/rpc"
import "fmt"
import "time"

import "crypto/rand"
import "math/big"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	server string
	me string
	out_count int
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.server = ck.vs.Primary()
	ck.me = (time.Now()).String()
	ck.out_count = 0

	return ck
}

func (ck *Clerk) UpdateClerk(){
	ck.server = ck.vs.Primary()
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)

	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	//fmt.Println("Get started")

	// Your code here.
	args := &GetArgs{}
	args.Key = key
	var reply GetReply 

	ck.out_count ++

	ok := false

	for ok == false{
		
		if ck.server == ""{
			ck.UpdateClerk()
			time.Sleep(viewservice.PingInterval)
		}
		//fmt.Println("call",ck.server)
		ok = call(ck.server, "PBServer.Get", args, &reply)

		if ok == false{
			time.Sleep(viewservice.PingInterval)
			ck.UpdateClerk()
		}

		Dprint(ck.vs.Primary(),ck.vs.Backup(),"Loop?")
	}

	if reply.Err == ErrNoKey{

	}

	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	args := &PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.Src = ck.me
	args.Sn = ck.out_count
	var reply PutAppendReply 

	ck.out_count ++

	//ok := 
	ret := false
	for i := 0; i < viewservice.DeadPings*3 && !ret; i++ {
		//fmt.Println(key," ",value," out...",i)
		if ck.server == ""{
			ck.UpdateClerk()
			time.Sleep(viewservice.PingInterval)
		}

		ret = call(ck.server, "PBServer.PutAppend", args, &reply)

		if ret == false{
			time.Sleep(viewservice.PingInterval)
			ck.UpdateClerk()
		}
		// fmt.Println(key, value, "-- i: ",i)
	}
	 // if !ret{
	 // 	fmt.Println(ck.server, " failed put: ", key, value)
	 // }else{
	 // 	fmt.Println(ck.server, " good put: ",key, value)
	 // }
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
