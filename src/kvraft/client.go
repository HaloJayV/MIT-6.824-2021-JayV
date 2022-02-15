package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

// Each client talks to the service through a Clerk with Put/Append/Get methods.
// A Clerk manages RPC interactions with the servers.
// 其实就是client需要通过clerk实例与service进行交互
type Clerk struct {
	// 保存服务器信息的数组
	Servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	// 客户端ID
	ClientId int64
	// 请求id
	RequestId int
	// 最近被client访问过的leaderId
	RecentLeaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 创建kvraft实例
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.Servers = servers
	// You'll have to add code here.
	// 每个client初始化唯一id
	ck.ClientId = nrand()
	// 负载均衡-随机
	ck.RecentLeaderId = GetRandomServer(len(ck.Servers))
	return ck
}

/*
Hint
After calling Start(), your kvservers will need to wait for Raft to complete agreement.
Commands that have been agreed upon arrive on the applyCh.
Your code will need to keep reading applyCh while PutAppend() and Get() handlers submit commands to the Raft log using Start().
Beware of deadlock between the kvserver and its Raft library.
调用Start()后，您的 kvserver 将需要等待 Raft 完成协议。已同意的命令到达applyCh。
当 PutAppend()和Get()处理程序使用Start()将命令提交到 Raft 日志时，
您的代码将需要继续读applyCh。注意 kvserver 和它的 Raft 库之间的死锁。

You are allowed to add fields to the Raft ApplyMsg, and to add fields to Raft RPCs such as AppendEntries,
however this should not be necessary for most implementations.
您可以向 Raft ApplyMsg添加字段，并向 Raft RPC（例如AppendEntries ）添加字段，
但是对于大多数实现而言，这不是必需的。

A kvserver should not complete a Get() RPC if it is not part of a majority (so that it does not serve stale data).
A simple solution is to enter every Get() (as well as each Put() and Append()) in the Raft log.
You don't have to implement the optimization for read-only operations that is described in Section 8.
如果 kvserver 不是多数的一部分，则不应完成Get() RPC（这样它就不会提供陈旧的数据）。
一个简单的解决方案是在 Raft 日志中输入每个Get()（以及每个Put() 和Append() ）。
您不必实现第 8 节中描述的只读操作的优化。

It's best to add locking from the start because the need to avoid deadlocks sometimes affects overall code design.
Check that your code is race-free using go test -race.
最好从一开始就加锁，可以避免死锁，有时还会影响整体代码设计。
使用go test -race检查您的代码是否无竞争。
*/
func GetRandomServer(length int) int {
	return mathrand.Intn(length)
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
	// You will have to modify this function.
	// 保证全局升序
	ck.RequestId++
	requestId := ck.RecentLeaderId
	server := ck.RecentLeaderId
	args := GetArgs{
		Key:       key,
		ClientId:  ck.ClientId,
		RequestId: requestId,
	}

	for {
		reply := GetReply{}
		// RPC请求KVServer的Get方法, 成功则返回leaderId
		ok := ck.Servers[server].Call("KVServer.Get", &args, &reply)
		// 换下一个Server，重试，直到 OK or Error
		if !ok || reply.Err == ErrWrongLeader {
			// LeaderId
			server = (server + 1) % len(ck.Servers)
			continue
		}

		if reply.Err == ErrNoKey {
			return ""
		}

		if reply.Err == OK {
			ck.RecentLeaderId = server
			return reply.Value
		}
	}
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
	// You will have to modify this function.
	ck.RequestId++
	requestId := ck.RequestId
	server := ck.RecentLeaderId
	for {
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClientId:  ck.ClientId,
			RequestId: requestId,
		}
		reply := PutAppendReply{}

		ok := ck.Servers[server].Call("KVServer.PutAppend", &args, &reply)

		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.Servers)
			continue
		}

		if reply.Err == ErrNoKey {
			return
		}

		if reply.Err == OK {
			ck.RecentLeaderId = server
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
