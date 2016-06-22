package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"


type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	view View
	P_ack bool
	state int
	rpccount int32
	P_ts, B_ts time.Time
}

const(
	N = 0
	P = 1
	PB = 2
	B = 3
)


//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	Dprint( args.Me, "pinged: ", vs.view.Primary, vs.view.Backup)

	switch vs.state {
		case N:
			vs.view.Primary = args.Me
			vs.P_ts = time.Now()
			vs.state = P
			vs.P_ack = true
			vs.view.Viewnum ++

		case P:
			if args.Me == vs.view.Primary{
				if args.Viewnum == 0{
					// Nothing left. Kill.
					vs.dead = true
					fmt.Println("Lone primary crashed... ",vs.me,"down")
					vs.l.Close()
				} else {
					if args.Viewnum == vs.view.Viewnum{
						vs.P_ack = true
					}
					vs.P_ts = time.Now()
					break
				}
			}
			if vs.P_ack == true && args.Viewnum != 0{
				vs.view.Backup = args.Me
				vs.B_ts = time.Now()
				vs.state = PB
				vs.view.Viewnum ++
				vs.P_ack = false
			}

		case PB:
			if args.Me == vs.view.Primary{
				if args.Viewnum == 0{
					// P crashed. Promote B right away.
					vs.view.Primary = vs.view.Backup
					vs.view.Backup = ""
					vs.state = P
					vs.P_ack = false
					//view.Viewnum++
				} else {
					if args.Viewnum == vs.view.Viewnum{
						vs.P_ack = true
					}
					vs.P_ts = time.Now()
					break
				}
			}
			if args.Me == vs.view.Backup{
				if args.Viewnum == 0{
					// B crashed.
					if vs.P_ack == true{
						vs.view.Backup = ""
						vs.state = P
						vs.view.Viewnum++
						vs.P_ack = false
					}
				} else {
					vs.B_ts = time.Now()
					break
				}
			}

	}

	//fmt.Printf("new vn = %d \n", view.Viewnum)
	reply.View = vs.view


	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.view
	//fmt.Println("vs.get handler involked. P = ",vs.view.Primary)

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	//fmt.Println("since ",time.Since(P_ts), " P_ack ",P_ack)

	// Your code here.
	if time.Since(vs.B_ts) > PingInterval * DeadPings{
		if vs.state == PB{
			vs.view.Backup = ""
			vs.state = P
			vs.view.Viewnum++
			vs.P_ack = false
		}else if vs.state == B{
			// Nothing left. Kill.
			vs.dead = true
			fmt.Println("Backup timed out. No valid server left...",vs.me,"down")
			vs.l.Close()
		}
	}

	if time.Since(vs.P_ts) > PingInterval * DeadPings{
		// Promote backup right aways (skip vs.state B)
		if vs.state == PB{
			if vs.P_ack{
				vs.view.Primary = vs.view.Backup
				vs.view.Backup = ""
				vs.state = P
				vs.view.Viewnum++
				vs.P_ack = false
			}
		}else if vs.state == P{
			// Nothing left. Kill.
			vs.dead = true
			fmt.Println("Primary timed out. No valid server left...",vs.me,"down")
			vs.l.Close()
		}
	}
	Dprint(vs.view.Primary, vs.view.Backup)

}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}


func StartServer(me string) *ViewServer {

	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.view = View {Viewnum: 0, Primary: "", Backup: ""}
	vs.P_ack = false
	vs.state = N
	vs.rpccount = 0

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func(){
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval*2)
		}
		fmt.Println(me, "went down")
	}()

	return vs
}
