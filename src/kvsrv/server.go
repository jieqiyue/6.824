package kvsrv

import (
	"fmt"
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

var ReqResTemplate = "%d-%d"

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store map[string]string

	// key为clientId，value为这个client的前面多少个请求都处理成功了
	//clientAck map[int64]int

	reqRes map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, ok := kv.store[args.Key]; ok {
		reply.Value = value
	} else {
		// 即使map中没有这个key，也要返回一个空字符串
		reply.Value = ""
	}
	//temp := fmt.Sprintf(ReqResTemplate, args.ClientId)
	//delete(kv.reqRes, temp)
	DPrintf("after master get function, get a result %s", reply.Value)
	// map操作先不更新clientAck
	return
}

func (kv *KVServer) FinishOp(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	temp := fmt.Sprintf(ReqResTemplate, args.ClientId, args.AckSeq)
	delete(kv.reqRes, temp)
	DPrintf("after master get function, get a result %s", reply.Value)
	// map操作先不更新clientAck
	return
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.CheckWriteOpNecessary(args, reply) {
		return
	}

	if value, ok := kv.store[args.Key]; ok {
		reply.Value = value
	} else {
		// 即使map中没有这个key，也要返回一个空字符串
		reply.Value = ""
	}

	kv.store[args.Key] = args.Value

	// 在处理成功之后，维护这个请求的client的状态
	kv.MaintainState(args, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.CheckWriteOpNecessary(args, reply) {
		return
	}

	if value, ok := kv.store[args.Key]; ok {
		reply.Value = value
		kv.store[args.Key] = value + args.Value
	} else {
		// 即使map中没有这个key，也要返回一个空字符串
		reply.Value = ""
		kv.store[args.Key] = args.Value
	}

	// 在处理成功之后，维护这个请求的client的状态
	kv.MaintainState(args, reply)
}

func (kv *KVServer) MaintainState(args *PutAppendArgs, reply *PutAppendReply) {
	reply.OpResult = true
	temp := fmt.Sprintf(ReqResTemplate, args.ClientId, args.AckSeq)
	kv.reqRes[temp] = reply.Value

	//temp1 := fmt.Sprintf(ReqResTemplate, args.ClientId, args.AckSeq-1)
	//if _, ok := kv.reqRes[temp1]; ok {
	//	delete(kv.reqRes, temp1)
	//}

	//fmt.Println("map len:", len(kv.reqRes))

	// 如果map中没有这个clientId，可以认为是这个client首次发送的请求，此时直接设置ackSeq
	//if _, ok := kv.clientAck[args.ClientId]; !ok {
	//	kv.clientAck[args.ClientId] = args.AckSeq
	//	return
	//}
	//
	//if _, ok := kv.clientAck[args.ClientId]; ok && kv.clientAck[args.ClientId] < args.AckSeq {
	//	kv.clientAck[args.ClientId] = args.AckSeq
	//	return
	//}
}

func (kv *KVServer) CheckWriteOpNecessary(args *PutAppendArgs, reply *PutAppendReply) bool {
	if args.AckSeq < 0 {
		DPrintf("%s", "CheckWriteOpNecessary got a less 0 request")
		return false
	}

	//if args.AckSeq == 0 {
	//	return true
	//}

	temp := fmt.Sprintf(ReqResTemplate, args.ClientId, args.AckSeq)
	if value, ok := kv.reqRes[temp]; ok {
		reply.Value = value
		reply.OpResult = true

		return false
	}

	if _, ok := kv.reqRes[temp]; !ok {
		return true
	}

	fmt.Println("CheckWriteOpNecessary un reach place.....")
	return true
	// 如果map中没有这个clientId，可以认为是这个client首次发送的请求，此时有必要进行OP操作
	//if _, ok := kv.clientAck[args.ClientId]; !ok {
	//	if args.AckSeq == 0 {
	//		return true
	//	}
	//
	//	fmt.Println("CheckWriteOpNecessary never get place...")
	//
	//	reply.OpResult = false
	//	return false
	//}
	//
	//// 如果map中存放的这个clientId最后处理的ackSeq大于请求过来的ackS，那么可以认为这个请求是重复请求，直接丢弃
	//if value, ok := kv.clientAck[args.ClientId]; ok {
	//	if value+1 == args.AckSeq {
	//		return true
	//	} else if args.AckSeq <= value {
	//		temp := fmt.Sprintf(ReqResTemplate, args.ClientId)
	//		if _, ok := kv.reqRes[temp]; !ok {
	//			fmt.Println("not happen!!!!in CheckWriteOpNecessary")
	//		}
	//		reply.Value = kv.reqRes[temp]
	//		reply.OpResult = true
	//		return false
	//	} else {
	//		fmt.Println("not happen twice !!!!in CheckWriteOpNecessary")
	//		reply.OpResult = false
	//		return false
	//	}
	//}
	//
	//DPrintf("error,CheckWriteOpNecessary unreached place...")
	//return true
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	//kv.clientAck = make(map[int64]int)
	kv.store = make(map[string]string)
	kv.reqRes = make(map[string]string)

	return kv
}
