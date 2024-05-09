package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry 用来表示每条日志
type LogEntry struct {
	Term  int         //  这个日志发出的leader的term是什么
	Index int         // 这个日志在整体日志中的位置
	Data  interface{} // 存储的LogEntry的数据
}

type ServerState int

const (
	UnknownState ServerState = iota // 未知状态
	Leader                          // 领导者
	Follower                        // 跟随者
	Candidate                       // 候选者
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your Data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	state      ServerState
	receiveNew bool

	startElectionTime time.Time
	electionInterval  int

	// persistent state
	currentTerm int        // 当前任期
	voteFor     int        // 当前任期的选票投给了哪个server
	log         []LogEntry // 当前这个server中的所有日志

	// volatile state
	commitIndex int // 当前可以应用到状态机的日志为止
	lastApplied int // 当前server已经应用到状态机的位置

	// volatile state on leader
	nextIndex  []int // 下一条要发送到各个client的日志的位置，指示的是log数组的下标
	matchIndex []int // 目前每个client已经匹配到的位置
}

// 不使用锁保护，需要调用者保证安全
func (rf *Raft) SetElectionTime() {
	rf.startElectionTime = time.Now()
	rf.electionInterval = rand.Intn(400) + 800

	return
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	isleader = false

	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(Data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 该函数是否需要先计算一下请求的args里面的日志和本地日志，哪个更新？先得出这个结论来，
	// 因为如果仅仅靠着大term，是不能够获取到选票的。获取选票的关键还是要看日志哪个更新。因为有可能一个网络分区的节点，一直在投票，导致自己的term
	// 非常大，然后忽然不分区了，重新加入集群之后，就会出现这种情况。
	// Your code here (3A, 3B).
	DPrintf("server[%d] recive %d server vote request, args Term is:%d", rf.me, args.CandidateId, args.Term)
	rf.mu.Lock()
	DPrintf("server[%d] recive %d server vote request, args Term is:%d, get lock success", rf.me, args.CandidateId, args.Term)
	defer func() {
		if reply.VoteGranted == true {
			//rf.receiveNew = true
			rf.SetElectionTime()
			rf.state = Follower
		}
		DPrintf("server[%d] has finish request vote, request server is:%d, args Term is:%d, result granted is:%v", rf.me, args.CandidateId, args.Term, reply.VoteGranted)
		rf.mu.Unlock()
	}()

	DPrintf("server[%d] begin to handler vote request, local Term is:%d, request server is:%d, request Term is:%d,", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	reply.Term = rf.currentTerm
	// 先判断两种特殊情况
	// 1. 如果请求的term小于当前server的term，则返回false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf("server[%d]not granted to server %d, because args Term less than me", rf.me, args.CandidateId)
		return
	}

	// 这种情况应该要先处理，就是当请求的term大于当前的term的时候
	if args.Term > rf.currentTerm {
		rf.AddCurrentTerm(args.Term)
	}

	// 2. 如果请求的term等于当前的term，并且自己当前的voteFor字段等于args里面的candidateId
	if args.Term == rf.currentTerm && rf.voteFor == args.CandidateId {
		rf.voteFor = args.CandidateId
		//rf.receiveNew = true
		reply.VoteGranted = true
		return
	}

	// 3. 如果当前term已经投票过了的话，那么就返回false,因为一个任期最多只能投票一次
	if rf.voteFor != -1 {
		reply.VoteGranted = false
		DPrintf("server[%d]not granted to server %d, because server has vote to %d", rf.me, args.CandidateId, rf.voteFor)
		return
	}

	// 接下来判断日志是否至少跟自己一样新
	localLastIndex := -1
	localLastTerm := -1

	if len(rf.log) != 0 {
		localLastIndex = rf.log[len(rf.log)-1].Index
		localLastTerm = rf.log[len(rf.log)-1].Term
	}

	if args.Term >= localLastTerm && args.LastLogIndex >= localLastIndex {
		// todo 这里还是直接设置一下true，减少无效的选举，这个设置receiveNew是否需要移动到defer里面去
		//rf.receiveNew = true
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		DPrintf("server[%d]not granted to server %d, because local Index is new than args", rf.me, args.CandidateId)
		reply.VoteGranted = false
	}
}

func (rf *Raft) RequestHeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		DPrintf("server[%d] got a heart beat, but args Term less than local Term, request Term is:%d, request server id is:%d", rf.me, args.Term, args.CandidateId)
		return
	}

	if args.Term == rf.currentTerm && rf.state == Candidate {
		DPrintf("server[%d] got a heart beat, local status is Candidate, but args server:%d send a heart beat,so change local state to follower", rf.me, args.CandidateId)
		rf.state = Follower
	}

	// 当请求的term大于本地的term的时候，就证明自己收到了更高的term的心跳，此时应该将自身转为follower，并且修改自身的term。
	if args.Term > rf.currentTerm {
		DPrintf("server[%d] got a heart beat, and args Term is bigger than local Term, args Term:%d, args server id:%d, local Term:%d", rf.me, args.Term, args.CandidateId, rf.currentTerm)
		rf.AddCurrentTerm(args.Term)
		return
	}

	// rf.receiveNew = true
	rf.SetElectionTime()
	DPrintf("server[%d]got heart beat, reset timeout, local Term is:%d, local status is:%d, args server id is:%d, args Term is:%d", rf.me, rf.currentTerm, rf.state, args.CandidateId, args.Term)
}

func (rf *Raft) RequestSendLog(args *SendLogArgs, reply *SendLogReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// 有两种情况会reply为false，一种是本地term大于args里面的term，一种是因为这个preLog对不上
	if rf.currentTerm > args.Term || args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 如果请求的term大于等于本地的term，则需要将自己转为follower，并且更新自己的term
	if args.Term > rf.currentTerm {
		rf.AddCurrentTerm(args.Term)
	}

	// 到这个位置的时候，当前server的term一定是等于args的term的，此处需要判断一下，如果当前server是一个候选者，则将他的状态改为follower
	if rf.state == Candidate {
		DPrintf("server[%d]got a append entries rpc, but current status is candidate,so change state to follower", rf.me)
		rf.state = Follower
		rf.voteFor = -1
	}

	// 判断这个位置是否有preLog的日志了，如果没有的话，返回false
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	// todo 如果当前server的状态是候选者状态，这个地方会不会影响他的选举？
	rf.SetElectionTime()
}

// 此方法不使用锁保护，防止锁重入出现panic，需要调用方保证线程安全性
func (rf *Raft) AddCurrentTerm(term int) {
	// 验证代码，一般不会出现这种情况
	if term <= rf.currentTerm {
		DPrintf("error, AddCurrentTerm function get a %d Term, current Term is %d, this is not happen", term, rf.currentTerm)
		return
	}

	// 新增任期，然后将term和state进行调整
	rf.currentTerm = term
	rf.state = Follower
	// 虽然这个voteFor没有明确在论文中指出需要设置为什么值，这里我考虑是需要设置为-1的
	rf.voteFor = -1
	//rf.receiveNew = true
	rf.SetElectionTime()
}

// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args *HeartBeatArgs, reply *HeartBeatReply) bool {
	ok := rf.peers[server].Call("Raft.RequestHeartBeat", args, reply)
	return ok
}

func (rf *Raft) sendLog(server int, args *SendLogArgs, reply *SendLogReply) bool {
	ok := rf.peers[server].Call("Raft.RequestSendLog", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
// Start() should return immediately, without waiting for the log appends to complete.
// 思路是在接收到上层的command之后，转化为一个Log，然后保存到本地。这里先直接返回，然后在另外一个专门分发Log的goroutine中
// 将这个日志fan out出去。另外那个专门分发Log的goroutine可以固定sleep 100ms，然后每次醒了之后，就判断自己是否是leader，
// 如果不是leader，则继续睡觉，如果是leader，则遍历rf里面的所有peer，给每一个follower和candidate发送日志。发送日志的时候，
// 给每一个不同的server启动一个新的goroutine来发送，这样就等于睡醒了之后，给每一个其他server发送完消息就不管了，继续去睡觉。如果其他server
// 有回应的话，就是在新建的那个goroutine中来处理的。比如说更新nextIndex，加锁去更新。这样就算某一个节点特别慢，也不会拖累整体的流程。因为每
// 一个server都是在独立的goroutine中进行处理的。并且就算某一个节点特别慢，导致第二次唤醒之后，又给他发了相同的日志，那么就需要他自己来保证这个
// 幂等性了。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return 0, rf.currentTerm, false
	}

	// 将command转化为一个LogEntry结构体
	log := LogEntry{
		Data: command,
		Term: rf.currentTerm,
	}

	// index还是先查看本地的Log数组中的最后一个LogEntry的index是什么，logIndex从1开始
	if len(rf.log) == 0 {
		log.Index = 0
	} else {
		// 如果本地的Log中已经有日志了，则新日志的index需要比原有的最后一条日志的index加一
		lastEntry := rf.log[len(rf.log)-1]
		log.Index = lastEntry.Index + 1
	}

	// 此时LogEntry已经构建好了，将它添加到Raft中的log数组中
	rf.log = append(rf.log, log)

	return log.Index, log.Term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heartBeat() {
	// 周期性的发送心跳消息，暂时不对返回值做处理
	shouldSendHeartBeat := false
	for rf.killed() == false {
		heartBeatArgs := HeartBeatArgs{}

		rf.mu.Lock()
		shouldSendHeartBeat = false
		if rf.state == Leader {
			shouldSendHeartBeat = true
			heartBeatArgs.CandidateId = rf.me
			heartBeatArgs.Term = rf.currentTerm
		}
		// for log
		serverId := rf.me
		serverState := rf.state
		//randNum := rand.Int63()
		// end
		rf.mu.Unlock()

		if shouldSendHeartBeat {
			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}

				go func(i int, args HeartBeatArgs) {
					reply := HeartBeatReply{}
					ok := rf.sendHeartBeat(i, &args, &reply)
					if !ok {
						// 针对已经不是leader的server还是会发心跳这个问题，可以把这个请求的args里面的term给打印出来，这样就能知道了
						DPrintf("server[%d] send heart beat to %d, bug get fail,i am a leader? status:%v, args Term is:%d", serverId, i, serverState, args.Term)
					}
					// todo 此处先不根据返回值来修改当前server的term值，有可能出现这样的情况：一个leader，独自发生了分区，然后他一直给
					// todo 其它的server发送心跳。此时其它server已经进行了新的选举了，所以其它server的term可能比当前server的大，但是
					// todo 此处并不根据这个心跳返回的term来更新本地server，而是希望通过新领导者的appendEntries或者是心跳请求来更新本地
					// todo 的term。
				}(i, heartBeatArgs)
			}
		}

		ms := 150
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendLogTicket() {
	// 周期性的发送日志
	shouldSendLog := false
	allSendLogArgs := make([]SendLogArgs, len(rf.peers))

	for rf.killed() == false {
		rf.mu.Lock()
		shouldSendLog = false
		// 如果当前是leader，并且自己含有日志的话，则将这次分发日志的参数都构建好
		// todo 这里直接使用rf.log来判断leader是否有日志要发送，后续引入了snapshot，这个地方是否需要更改？
		if rf.state == Leader && len(rf.log) > 0 {
			shouldSendLog = true

			for i, _ := range rf.peers {
				sendLogArgs := SendLogArgs{}
				// 此处不能使用append，要使用下标的方式每次都覆盖上次这个位置的值
				allSendLogArgs[i] = sendLogArgs

				// 如果这个follower的nextIndex是leader的最后一条日志的index + 1的位置，那么就认为没有新的日志要发送
				// 所以这个nextIndex一开始需要初始化为len(rf.log) - 1的位置，表示将要发送的Log的下标。如果一开始的时候，leader一条日志都
				// 没有，那么会初始化为0，否则初始化为len(rf.log) - 1.当初始化为0的时候，这个条件也会成立，所以也不会再发送RPC了。而当leader
				// 后续添加了一条log之后，len(rf.log) 就变成1了，此时的nextIndex还是0，那么不成立也就会继续往下去发送日志了
				if i == rf.me || rf.nextIndex[i] >= len(rf.log) {
					sendLogArgs.ShouldSend = false
					continue
				}

				// log
				DPrintf("server[%d] prepare send log to %d, the nextIndex is:%d, len of log is:%d", rf.me, i, rf.nextIndex[i], len(rf.log))
				if rf.nextIndex[i] < 0 {
					DPrintf("error,server[%d] found %d nextIndex less than 0", rf.me, i)
					rf.nextIndex[i] = 0
				}

				// todo 此时是否需要判断nextIndex是否是大于0的，有可能某一个follower的nextIndex一直减少，一直到0以下了
				sendLogArgs.ShouldSend = true
				sendLogArgs.LeaderId = rf.me
				sendLogArgs.Term = rf.currentTerm

				// 到这个位置的话，leader的log一定是含有nextIndex[i]这个下标位置的log的，并且nextIndex[i]不可能会小于0，至少都是0，
				// 而当如果nextIndex[i]是0的话，那么是没有preLog的，而当nextIndex[i]大于0的话，则一定有前一条日志。
				if rf.nextIndex[i] == 0 {
					sendLogArgs.PrevLogTerm = -1
					sendLogArgs.PrevLogIndex = -1
				} else {
					preLog := rf.log[rf.nextIndex[i]-1]
					sendLogArgs.PrevLogTerm = preLog.Term
					sendLogArgs.PrevLogTerm = preLog.Term
				}

				sendLogArgs.Entries = rf.log[rf.nextIndex[i]:]
				sendLogArgs.LeaderCommit = rf.commitIndex
			}
		}

		rf.mu.Unlock()

		if shouldSendLog {
			for i, _ := range rf.peers {
				if i == rf.me || !allSendLogArgs[i].ShouldSend {
					continue
				}

				go func(i int) {
					reply := SendLogReply{}
					//  for update commitIndex,这里不能用args的term，而是应该用当前leader的term，因为日志的term有可能是之前的term的，
					// 而这里要拿到的是当前发出这个请求的leader的term。
					leaderTerm := allSendLogArgs[i].Term
					lastSendLogIndex := allSendLogArgs[i].Entries[len(allSendLogArgs[i].Entries)-1].Index
					lastSendLogTerm := allSendLogArgs[i].Entries[len(allSendLogArgs[i].Entries)-1].Term
					// for log
					preLogTerm := allSendLogArgs[i].PrevLogTerm
					preLogIndex := allSendLogArgs[i].PrevLogIndex

					ok := rf.sendLog(i, &allSendLogArgs[i], &reply)
					if !ok {
						DPrintf("server[%d] send log to %d, bug get fail, args Term is:%d, args Index is:%d", rf.me, i, preLogTerm, preLogIndex)
						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.AddCurrentTerm(reply.Term)
						return
					}

					// 在更新之前，还需要进行两个判断，当前是否是leader，并且当前的term是否还是之前发出这个请求的时候的term，因为可能出现如下情形：
					// 1. 如果当前已经不是leader了，那么此时再去更新commitIndex，并且把已经提交的日志发送到上层的话，会造成误解，因为这个位置的日志可能
					// 已经被新的term的leader重写了。此时可能会提交一个错误的index的日志。
					// 2. 如果当前是leader，但是当前的term和发出请求的term不相同。那么可能中间没有当leader的那个term可能已经把这个位置的日志重写了。
					// 那么此时如果更新nextIndex的话，就更新不是自己term的了，会影响最新的term的nextIndex了。
					if rf.state == Leader && rf.currentTerm == leaderTerm {
						// 更新这个follower的nextIndex，和leader的commitIndex
						if reply.Success {
							// 更新nextIndex,matchIndex也更新为这次请求设置的值，这个地方还需要判断是否之前的大，如果大的话，才去更新，
							// 因为有可能这次请求执行的很慢，导致leader下一次重发了这个请求，并且重发了后面的请求，然后后面的请求都已经执行成功了。
							// 所以matchIndex已经被更新到更大的值了，此时如果不判断直接设置的话，会导致matchIndex回退。
							if lastSendLogIndex > rf.matchIndex[i] {
								rf.matchIndex[i] = lastSendLogIndex
							}
							if lastSendLogIndex+1 > rf.nextIndex[i] {
								rf.nextIndex[i] = lastSendLogIndex + 1
							}

							// 更新commitIndex，注意这里不能提交之前term的日志，只能提交本term的日志
							matchCount := 0
							for _, matchIndex := range rf.matchIndex {
								if matchIndex >= rf.matchIndex[i] {
									matchCount++
								}
							}

							// 注意这里要判断这个日志的term是否是本leader的term，如果不是则不提交
							if rf.matchIndex[i] > rf.commitIndex && matchCount > len(rf.peers)/2 && rf.currentTerm == lastSendLogTerm {
								rf.commitIndex = rf.matchIndex[i]

							}
						} else {
							// 如果返回失败，则需要递减nextIndex的值,往前退一个值，正常来说，对于index为0的日志，是不可能会添加失败的。
							// follower接收到下标为0的日志的时候，应该是要无条件接收的。
							// 如果这次的log并没有分发成功，则对于这个请求来说，不需要去更新leader的commitIndex。
							rf.nextIndex[i]--
							// todo 这里暂时不更新matchIndex，而去依赖当reply为success的时候去更新
						}
					}
				}(i)
			}
		}

		ms := 150
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	shouldElection := false
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		// 此处只需要args，args可以共用，但是reply得是每一个goroutine新建立的
		var args RequestVoteArgs

		rf.mu.Lock()
		shouldElection = false

		DPrintf("server[%d]begin a new ticker, local state is:%v, local Term is:%d, and time is bigger than interval?:%v",
			rf.me, rf.state, rf.currentTerm, time.Since(rf.startElectionTime) > time.Duration(rf.electionInterval)*time.Millisecond)
		// todo  此处是否是这两个状态呢？如果某一个节点是候选者，并且在某一次选举中，并没有成为follower的话，就需要继续选举
		// todo 所以这里也依赖这个receiveNew，这个receiveNew需要在接收到新heartBeat或者是appendEntries的时候更新
		if time.Since(rf.startElectionTime) > time.Duration(rf.electionInterval)*time.Millisecond && (rf.state == Follower || rf.state == Candidate) {
			DPrintf("server[%d]should begin a election", rf.me)
			shouldElection = true
			rf.currentTerm++
			rf.state = Candidate
			rf.voteFor = rf.me
			DPrintf("server[%d]has vote for himself, Term is:%d", rf.me, rf.currentTerm)

			args = RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}

			// 如果当前是空日志的话
			if len(rf.log) == 0 {
				args.LastLogIndex = -1
				// todo 此处应该要设置为0，还是用rf里面的currentTerm
				args.LastLogTerm = -1
			} else {
				tempLog := rf.log[len(rf.log)-1]
				args.LastLogTerm = tempLog.Term
				args.LastLogIndex = tempLog.Index
			}
		}
		rf.mu.Unlock()

		// 在发送RPC之前释放锁，防止死锁
		go func() {
			if shouldElection {
				// 由于RPC已经含有了超时的功能，所以此处直接使用waitGroup，不用担心某一个节点不可达，导致reply一直收不到的情况
				//wg := sync.WaitGroup{}
				cond := sync.NewCond(&rf.mu)
				finishRequest := 1
				voteCount := 1
				rpcFailCount := 0

				for i, _ := range rf.peers {
					if i == rf.me {
						continue
					}

					//wg.Add(1)
					go func(i int, args RequestVoteArgs) {
						// 此处的reply必须每个goroutine都使用不同的
						reply := RequestVoteReply{}
						// 注意这里的i要使用外部传入的参数，不能直接使用闭包
						ok := rf.sendRequestVote(i, &args, &reply)

						DPrintf("server[%d] got server %d vote respone, args Term is:%d,reply grante:%v, is ok? %v, not get lock", rf.me, i, args.Term, reply.VoteGranted, ok)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						finishRequest++
						DPrintf("server[%d] got server %d vote respone, args Term is:%d,reply grante:%v, is ok? %v, get lock success", rf.me, i, args.Term, reply.VoteGranted, ok)
						if ok {
							if reply.VoteGranted {
								voteCount++
							}

							if reply.Term > rf.currentTerm {
								DPrintf("server[%d]request for leader, but server:%d, "+
									"response Term:%d is bigger than local Term:%d, so change local Term to new", rf.me, i, reply.Term, rf.currentTerm)
								rf.AddCurrentTerm(reply.Term)
							}
						} else {
							rpcFailCount++
						}
						cond.Broadcast()
					}(i, args)
				}

				//wg.Wait()
				rf.mu.Lock()
				// 退出循环的条件
				// 1. 过半节点同意   2. 所有请求都已经回复了
				// 考虑一个3server组成的一个集群的情况：分别是 0，1，2三个机器。0机器是离线的，分区了。然后集群中只有1，2两台机器能互相通信。此时1开始选举，选举的term为5，2也开始选举，term也为5，
				// 原来的判断条件中，需要所有的节点都回复才能跳出循环。
				for voteCount <= (len(rf.peers)/2) && finishRequest < len(rf.peers) && voteCount+(len(rf.peers)-finishRequest) > (len(rf.peers)/2) {
					cond.Wait()
					DPrintf("server[%d] wait walk up, voteCount is:%d, finishRequest:%d,len/2 is:%d, vote plus finish is:%d", rf.me, voteCount, finishRequest, len(rf.peers)/2, voteCount+(len(rf.peers)-finishRequest))
				}
				//DPrintf("server[%d]is going to sleep.....555", rf.me)
				DPrintf("server[%d], vote for leader, got %d tickets, got %d failRpc, total %d request, args Term is:%d", rf.me, voteCount, rpcFailCount, finishRequest, args.Term)
				// 接下来要根据投票的结果来修改本地的状态了
				if rf.currentTerm == args.Term && rf.state == Candidate {
					if voteCount > len(rf.peers)/2 {
						rf.state = Leader
						DPrintf("server[%d], become a leader, got %d tickets", rf.me, voteCount)
					}
				}
				//DPrintf("server[%d]is going to sleep.....444", rf.me)
				rf.mu.Unlock()
				//DPrintf("server[%d]is going to sleep.....333", rf.me)
			}
		}()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// 需要在这里重置一下是否收到新消息,防止receiveNew一直为true，然后不执行新的选举
		// 这个是不是只能放在外面吧。
		//DPrintf("server[%d]is going to sleep.....111", rf.me)
		//rf.mu.Lock()
		//rf.receiveNew = false
		//rf.mu.Unlock()

		//DPrintf("server[%d]is going to sleep.....222", rf.me)
		/*
			如果总结哪些地方需要更新receiveNew呢？
			1. 收到投票请求，然后自己投了赞成票了，此时需要重置定时器，如果发现请求投票的server的term比自己小的话，是不会投票的，
				此时也不需要重置定时器。
			2. 作为一个follower，当收到leader发来的消息的时候
		*/

		ms := 150 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).

	// 启动的时候，服务器应该是跟随者的状态
	rf.state = Follower
	rf.currentTerm = 0
	// todo 此处不能设置为true，不然第一个测试会过不了
	//rf.receiveNew = true
	//rf.SetElectionTime()
	rf.startElectionTime = time.Now()
	rf.electionInterval = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.voteFor = -1

	// 初始化日志分发相关的切片
	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartBeat()

	return rf
}
