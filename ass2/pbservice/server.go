package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "../viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

const(
	PRIMARY = 1
	BACKUP = 2
	OLD_PRIMARY = 3
	IDLE = 4
)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	primary string
	backup string
	viewnum uint
	state	int
	db	map[string]string	
	cache	map[string]string
	rpc_log map[string]int // src clerk as the key, S/N as the value	
}

func (pb *PBServer) Sync(bServer string) error{

	pb.mu.Lock()
	defer pb.mu.Unlock()

	args := &SyncArgs{}
	args.DB = make(map[string]string)
	args.LastElem = make(map[string]string)
	for key, value := range pb.db{
		args.DB[key] = value
	}
	for key, value := range pb.cache{
		args.LastElem[key] = value
	}

	var reply SyncReply

	ok := call(bServer, "PBServer.SyncHlr", args, &reply)

	if !ok{
		//fmt.Println("Sync not ok!")
	}

	return nil
}

func (pb *PBServer) SyncHlr(args *SyncArgs, reply *SyncReply) error{

	pb.mu.Lock()
	for key, value := range args.DB{
		pb.db[key] = value
		//fmt.Println(pb.me, ": synced ", key, args.LastElem[key])
	}
	pb.mu.Unlock()

	return nil
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

	// pb.mu.Lock()
	// defer pb.mu.Unlock()

	// redirect to new primary if I am no longer primary
	if pb.state != PRIMARY{
		call(pb.vs.Primary(),"PBServer.Get",args, reply)
		reply.Forwarded = true
	}else{
		reply.Value = pb.db[args.Key]
		reply.Forwarded = false
	}

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	//Your code here.

	pb.mu.Lock()
	defer pb.mu.Unlock()


	// Check if incoming message is redundent
	val, ok := pb.rpc_log[args.Src]
	if ok && (val >= args.Sn) {
		reply.Err = ErrRepeatedRPC
		return nil
	}

	pb.rpc_log[args.Src] = args.Sn

	// Forwarding to backup
	if pb.state == PRIMARY{
		// view, ok := pb.vs.Get()
		// var backup string
		// if ok {
		// 	backup = view.Backup
		// }else{backup = ""}

		if pb.backup != ""{
			call(pb.backup,"PBServer.PutAppend",args, reply)
			//fmt.Println(pb.me, ": forwarding", args.Key, args.Value)
		}
	}

	//fmt.Println(args.Key," ",args.Value,"in...")
	pb.cache[args.Key] = args.Value
	_,ok = pb.db[args.Key]
	if ok == true && args.Op == "Append"{
		pb.db[args.Key] += args.Value
		//fmt.Println(pb.me,": append...")
	}else{
		pb.db[args.Key] = args.Value
		//fmt.Println(pb.me,": put...")
	}

	reply.Err = OK

	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	// Your code here.
	// args := &viewservice.PingArgs{}
	// args.Me = pb.me
	// args.Viewnum = 0
	// var reply viewservice.PingReply

	// ok := call(pb.vs.server, "ViewServer.Ping", args, &reply)
	// if ok == true {
	// 	if reply.View.Primary == pb.me {
	// 		pb.state = PRIMARY
	// 	}else if reply.View.Backup == pb.me {
	// 		pb.state = BACKUP
	// 	}else {
	// 		pb.state = IDLE
	// 	}
	// }
	Dprint(pb.me,"pinging")
	view,e := pb.vs.Ping(pb.viewnum)
	if e != nil{
		Dprint("Ping failed")
	}else{
		Dprint(view.Primary,view.Backup)
	}
	pb.primary = view.Primary
	pb.backup = view.Backup
	pb.viewnum = view.Viewnum
	if view.Primary == pb.me{
		pb.state = PRIMARY
		if view.Backup != ""{
			pb.Sync(pb.backup)
		}
	}else if view.Backup == pb.me {
		pb.state = BACKUP
	}else{
		pb.state = IDLE
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	Dprint("Killing", pb.me)
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {

	fmt.Println(me, "online")
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.state = IDLE
	pb.db = make(map[string]string)
	pb.cache = make(map[string]string)
	pb.rpc_log = make(map[string]int)
	pb.viewnum = 0

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		 fmt.Println(pb.me,"went down")
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
