package raft

import (
	"bytes"
	"fmt"
	"github.com/0990/goraft/labgob"
	"github.com/0990/goraft/rpcutil"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
}

type LogEntry struct {
	Command any
	Term    int
	Index   int
}

type CommandState struct {
	Term  int
	Index int
}

const (
	FOLLOWER    = 0
	CANDIDATE   = 1
	LEADER      = 2
	CopyEntries = 3
	HeartBeat   = 4
)

type Raft struct {
	mu        sync.Mutex
	peers     []*rpcutil.ClientEnd
	persister *Persister
	me        int
	dead      int32

	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	identify     int
	peersLen     int
	hbCnt        int
	applyCh      chan ApplyMsg
	doAppendCh   chan int
	applyCmdLogs map[any]*CommandState
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v: 获取 %v 的状态", rf.currentTerm, rf.me)

	term = rf.currentTerm
	isLeader = rf.identify == LEADER
	return term, isLeader
}

// 将raft的状态进行持久化
// 需要持久化的数据有votedFor, currentTerm, 以及对应的日志logs
func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		encoder.Encode(rf.votedFor)
		encoder.Encode(rf.currentTerm)
		encoder.Encode(rf.logs)
		DPrintf("%v: %v 持久化了 votedFor=%v, logs 的最后一个是 %v",
			rf.currentTerm, rf.me, rf.votedFor, rf.logs[len(rf.logs)-1])
	}()

	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	reader := bytes.NewReader(data)
	decoder := labgob.NewDecoder(reader)
	var votedFor, currentTerm int
	var logs []LogEntry
	err1 := decoder.Decode(&votedFor)
	err2 := decoder.Decode(&currentTerm)
	err3 := decoder.Decode(&logs)
	if err1 != nil || err2 != nil || err3 != nil {
		log.Fatalf("%v resume fail err1=%v,err2=%v,err3=%v", rf.me, err1, err2, err3)
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.logs = logs

		for i := range rf.logs {
			logEntry := rf.logs[i]
			rf.applyCmdLogs[logEntry.Command] = &CommandState{
				Term:  logEntry.Term,
				Index: logEntry.Index,
			}
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	PrevLogIndex int
}

type RequestVoteArgs struct {
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的候选人的 Id
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("%v: %v 认为 %v 的投票请求 {term=%v, lastIdx=%v, lastTerm=%v} 过时了",
			rf.currentTerm, rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
		return nil
	}

	if args.Term > rf.currentTerm {
		DPrintf("%v: %v 接收到投票请求后发现自己过时，变回追随者，新的任期为 %v", rf.currentTerm, rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.identify = FOLLOWER
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLog := rf.logs[len(rf.logs)-1]
		if args.LastLogTerm > lastLog.Term {
			rf.identify = FOLLOWER
			rf.votedFor = args.CandidateId
			rf.hbCnt++
			reply.VoteGranted = true
		} else if args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index {
			rf.identify = FOLLOWER
			rf.votedFor = args.CandidateId
			rf.hbCnt++
			reply.VoteGranted = true
		}
	}

	go rf.persist()
	DPrintf("%v: %v 对 %v 的投票 {term=%v, lastIdx=%v, lastTerm=%v} 结果为 %v, votedFor=%v",
		rf.currentTerm, rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, reply.VoteGranted, rf.votedFor)
	return nil
}

// AppendEntries函数，有两个功能，HeartBeat和log replication功能
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.PrevLogIndex = args.PrevLogIndex
	// 对于所有的Raft节点，如果发现自己的Term小于leader的任期，则更新自己的任期, 将votedFor变为args.LeaderId, 并将自己的状态变为FOLLOWER
	if args.Term > rf.currentTerm {
		DPrintf("%v: %v 收到条目增加请求后发现自己过时，变回追随者，新任期为 %v", rf.currentTerm, rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.identify = FOLLOWER
	}

	// 当前服务器的日志长度
	lenLogsCurent := len(rf.logs)
	// 如果当前Raft节点的Term大于leader的Term，则直接返回
	if args.Term < rf.currentTerm {
		if len(args.Entries) > 0 {
			DPrintf("%v: %v 接收到 %v 的复制请求 {{%v ~ %v} term=%v leaderCommit=%v}，结果为：已过时",
				rf.currentTerm, rf.me, args.LeaderId, args.Entries[0], args.Entries[len(args.Entries)-1],
				args.Term, args.LeaderCommit)
		} else {
			DPrintf("%v: %v 接收到 %v 的复制请求 {term=%v leaderCommit=%v}，结果为：已过时",
				rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderCommit)
		}
		return nil
	}

	// 如果PrevLogIndex大于当前服务器的日志长度, 或者不匹配
	if args.PrevLogIndex >= lenLogsCurent || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		if len(args.Entries) > 0 {
			DPrintf("%v: %v 接收到 %v 的复制请求 {{%v ~ %v} term=%v leaderCommit=%v}，结果为：不匹配",
				rf.currentTerm, rf.me, args.LeaderId, args.Entries[0], args.Entries[len(args.Entries)-1],
				args.Term, args.LeaderCommit)
		} else {
			DPrintf("%v: %v 接收到 %v 的复制请求 {term=%v leaderCommit=%v}，结果为：不匹配",
				rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderCommit)
		}

		if args.PrevLogIndex >= lenLogsCurent {
			reply.PrevLogIndex = lenLogsCurent
			return nil
		}

		// 如果不匹配的话，将当前term的日志全都删除掉, 然后设置PrevLogIndex
		i := args.PrevLogIndex
		term := rf.logs[i].Term
		for i--; i >= 0 && rf.logs[i].Term == term; i-- {
		}
		reply.PrevLogIndex = i + 1
		return nil
	}

	var deleteLogEntries []LogEntry
	idx1 := args.PrevLogIndex + 1
	idx2 := len(args.Entries) - 1
	for idx1 < lenLogsCurent && idx2 >= 0 {
		log1 := &rf.logs[idx1]
		log2 := args.Entries[idx2]
		if log1.Term != log2.Term || log1.Index != log2.Index {
			deleteLogEntries = rf.logs[idx1:]
			rf.logs = rf.logs[:idx1]
			break
		}
		idx1++
		idx2--
	}

	// 删除多余的日志, 并将相应的命令也删除
	for i := 0; i < len(deleteLogEntries); i++ {
		delete(rf.applyCmdLogs, deleteLogEntries[i].Command)
	}

	for idx2 >= 0 {
		logEntry := &rf.logs[idx2]
		rf.logs = append(rf.logs, *logEntry)
		rf.applyCmdLogs[logEntry.Command] = &CommandState{
			Term:  logEntry.Term,
			Index: logEntry.Index,
		}
		idx2--
	}

	if rf.commitIndex < args.LeaderCommit {
		idx := len(rf.logs) - 1
		if args.LeaderCommit <= idx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = idx
		}

		go rf.apply()
	}

	rf.hbCnt++
	reply.Success = true

	go rf.persist()
	if len(args.Entries) > 0 {
		DPrintf("%v: %v 接收到 %v 的复制请求 {{%v ~ %v} term=%v leaderCommit=%v}，结果为：成功",
			rf.currentTerm, rf.me, args.LeaderId, args.Entries[0], args.Entries[len(args.Entries)-1],
			args.Term, args.LeaderCommit)
	} else {
		DPrintf("%v: %v 接收到 %v 的复制请求 {term=%v leaderCommit=%v}，结果为：成功",
			rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderCommit)
	}
	//DPrintf("%v: %v 接收到 %v 的增加条目(%v)请求，结果为 %v", rf.currentTerm, rf.me, args.LeaderId, *args, reply.Success)

	return nil
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("----- %v 结束 -----", rf.me)
	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("%v: %v 的日志内容：%v", rf.currentTerm, rf.me, rf.logs)
	}()
}

// 判断当前Raft节点是否宕机
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Raft节点执行命令, 调用此接口来是Raft节点执行对应的命令, command为具体的命令
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = !rf.killed() && rf.identify == LEADER
	term = rf.currentTerm

	// Raft中所有的操作都是需要进过leader进行操作的
	// 如果是leader, 就进行相应的操作
	if isLeader {
		if commandState, has := rf.applyCmdLogs[command]; has {
			return commandState.Index, commandState.Term, isLeader
		}

		index = len(rf.logs)
		logEntry := LogEntry{
			Command: command,
			Term:    term,
			Index:   index,
		}

		rf.logs = append(rf.logs, logEntry)
		rf.applyCmdLogs[command] = &CommandState{
			Term:  term,
			Index: index,
		}
		go rf.persist()
		go func() {
			// 表示需要进行日志的复制, 放到管道中
			rf.doAppendCh <- CopyEntries
		}()
		DPrintf("-----%v 是领导，增加了新的条目为 {%v %v %v}", rf.me, command, term, index)
	}

	return index, term, isLeader
}

func (rf *Raft) SendLogEntry(flag int) {
	rf.mu.Lock()

	if rf.identify != LEADER {
		rf.mu.Unlock()
		return
	}

	argsTemplate := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	loglen := len(rf.logs)
	//if flag == CopyEntries {
	DPrintf("%v: %v 开始复制，最后一个条目为 %v，最后提交的索引为 %v",
		rf.currentTerm, rf.me, rf.logs[loglen-1], rf.commitIndex)
	//}
	rf.mu.Unlock()

	resultCh := make(chan int, rf.peersLen)
	wg := sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := argsTemplate
			preIdx := func() int {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				curNextIndex := loglen - 1
				// 根据nextIndex[i], 将nextIndex[i]之后的日志加入到args.Entries中
				for ; curNextIndex >= rf.nextIndex[i]; curNextIndex-- {
					args.Entries = append(args.Entries, rf.logs[curNextIndex])
				}
				return curNextIndex
			}()
			// 不断减少preIdx来进行AppendEntries的RPC通信, leader和follower之间日志的对齐
			for preIdx >= 0 {
				rf.mu.Lock()
				if rf.identify != LEADER || preIdx >= len(rf.logs) {
					rf.mu.Unlock()
					resultCh <- -2
					break
				}

				args.PrevLogIndex = preIdx
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					resultCh <- 2
					break
				}

				if reply.Term > args.Term {
					resultCh <- reply.Term
					break
				}

				if reply.Success {
					rf.mu.Lock()
					rf.nextIndex[i] = loglen
					rf.mu.Unlock()
					resultCh <- -1
					break
				} else {
					rf.mu.Lock()
					if preIdx >= len(rf.logs) {
						rf.mu.Unlock()
						break
					}

					rf.nextIndex[i] = reply.PrevLogIndex
					for ; preIdx >= reply.PrevLogIndex; preIdx-- {
						args.Entries = append(args.Entries, rf.logs[preIdx])
					}
					rf.mu.Unlock()
				}
			}
		}(i)
	}

	grantedCount := 1
	notGrantedCount := 0

	tgt := rf.peersLen / 2
	for finish := 1; finish < rf.peersLen; finish++ {
		result := <-resultCh
		rf.mu.Lock()
		if rf.identify != LEADER {
			rf.mu.Unlock()
			break
		}

		if rf.currentTerm != argsTemplate.Term {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		if result == -1 {
			grantedCount++
			if grantedCount > tgt {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					commit := loglen - 1
					// 如果当前commitIdx小于commit, 则将当前已经commit的日志apply到状态机进行执行
					if rf.identify == LEADER && commit < len(rf.logs) && commit > rf.commitIndex {
						rf.commitIndex = commit
						go rf.apply()
						//if flag == CopyEntries {
						DPrintf("%v: %v 提交成功，提交的最大的索引为 %v，最后复制过去的是 %v",
							argsTemplate.Term, rf.me, rf.commitIndex, rf.logs[commit])
						//}
					}
				}()
				break
			}
		} else if result == -2 {
			notGrantedCount++
			// 如果复制失败的节点数大于n / 2 + 1, 则表示当前提交失败, 当前leader变为follower
			if notGrantedCount > tgt {
				DPrintf("%v: %v 提交失败，准备提交的索引为 %v，退回追随者", argsTemplate.Term, rf.me, loglen-1)
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.identify = FOLLOWER
				}()
				break
			}
		} else if result > argsTemplate.Term {
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm < result {
					rf.currentTerm = result
					rf.votedFor = -1
					rf.identify = FOLLOWER
					DPrintf("%v: %v 收到条目增加响应后发现自己过时，变成追随者，新任期为 %v",
						argsTemplate.Term, rf.me, rf.currentTerm)
				}
			}()
			break
		} else {
			panic("出现一个意外的值： result=" + string(result))
		}
	}
}

func (rf *Raft) setToFollower() {
	rf.identify = FOLLOWER
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果最近一次提交的日志索引小于状态机执行日志索引，则是异常状态
	// 因为只有commited状态的日志才能被apply到状态机进行执行
	if rf.commitIndex < rf.lastApplied {
		log.Fatalf("%v: %v 调用 apply()： commitIndex(%v) < lastApplied(%v)",
			rf.currentTerm, rf.me, rf.currentTerm, rf.lastApplied)
	}

	// 如果最近一次提交的日志索引和状态机执行的日志索引相等，则没有需要执行的日志
	if rf.commitIndex == rf.lastApplied {
		return
	}

	// 只有commitIdx大于lastApplied的时候才是正常状态
	// 此时执行循环，将所有commited状态的日志全部apply状态机进行执行
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		logEntry := rf.logs[rf.lastApplied]
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: logEntry.Index,
		}
	}
	DPrintf("%v: %v 最后应用的是 %v, logs 里最后一个是 %v",
		rf.currentTerm, rf.me, rf.logs[rf.lastApplied], rf.logs[len(rf.logs)-1])
}

// 生成一个随机超时时间
func randomTimeout(min, max int) int {
	return rand.Intn(max-min) + min
}

func MakeRaft(peers []*rpcutil.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	fmt.Printf("----- %v 开始 -----\n", rf.me)
	rf.votedFor = -1
	rf.peersLen = len(peers)
	rf.logs = append(rf.logs, LogEntry{
		Command: "start",
		Term:    0,
		Index:   0,
	})
	// 首先将当前服务器设置为Follower
	rf.setToFollower()
	rf.commitIndex = 0
	// 创建nextIndex, int数组, 数组长度为服务器的数量
	rf.nextIndex = make([]int, rf.peersLen)
	// 创建matchIndex, int数组, 数组长度为服务器的数量
	rf.matchIndex = make([]int, rf.peersLen)
	rf.applyCh = applyCh
	rf.doAppendCh = make(chan int, 256)
	// 存储命令的字典
	rf.applyCmdLogs = make(map[interface{}]*CommandState)
	// 随机时间选取
	rand.Seed(time.Now().UnixNano())

	go func() {
		for {
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.identify {
			case FOLLOWER:
				oldCnt := rf.hbCnt
				rf.mu.Unlock()

				timeout := time.Duration(randomTimeout(7000000, 10000000)) * time.Millisecond
				if oldCnt < 10 {
					timeout = time.Duration(randomTimeout(700, 1000)) * time.Millisecond
				}
				time.Sleep(timeout)
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// 这里表示如果超过一段时间没有leader发送HeartBeat信号, 则进行一轮新的选举
					// 只有当当前任期的leader过期才能进行下一次的选举
					if rf.hbCnt == oldCnt {
						rf.identify = CANDIDATE
						rf.currentTerm++
						rf.votedFor = rf.me
						go rf.persist()
						fmt.Printf("%v: %v 的心跳超时 (%v)", rf.currentTerm, rf.me, timeout)
					}
				}()
			case CANDIDATE:
				fmt.Printf("%v: %v 开始竞选", rf.currentTerm, rf.me)
				rf.mu.Unlock()
				wonCh := make(chan int, 2)
				wg := sync.WaitGroup{}
				wg.Add(1)
				go func() {
					wg.Wait()
					close(wonCh)
				}()
				wg.Add(1)
				go rf.leaderElection(wonCh, &wg)

				// 设置一个超时时间
				timeout := time.Duration(randomTimeout(1000, 1400)) * time.Millisecond
				wg.Add(1)
				go func() {
					// 这里进行超时操作
					time.Sleep(timeout)
					wonCh <- 2
					wg.Done()
				}()

				// 如果最后wonCh通道返回的结果是2, 说明超时时间到了, 但是选举依然没有完成, 进行新一轮的选举
				res := <-wonCh
				// 选举超时
				if 2 == res {
					fmt.Printf("%v: %v 竞选超时 (%v)\n", rf.currentTerm, rf.me, timeout)
					rf.mu.Lock()
					rf.votedFor = rf.me
					rf.currentTerm++
					rf.mu.Unlock()
					go rf.persist()
				}
				wg.Done()
			default:
				DPrintf("当前Leader是: %d\n", rf.me)
				rf.mu.Unlock()
				rf.doAppendCh <- HeartBeat
				time.Sleep(time.Second * 5000)
			}
		}
	}()

	go func() {
		for {
			if rf.killed() {
				return
			}
			// 从doAppendCh管道中读取leader需要和follower进行的通信种类
			rf.SendLogEntry(<-rf.doAppendCh)
		}
	}()

	// 从持久化对象中读取状态
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) leaderElection(wonCh chan int, wgp *sync.WaitGroup) {
	defer wgp.Done()

	args := &RequestVoteArgs{}
	args.CandidateId = rf.me
	rf.mu.Lock()

	lcTerm := rf.currentTerm
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.logs) - 1
	args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	rf.mu.Unlock()
	c := make(chan *RequestVoteReply, rf.peersLen)
	wg := sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()
	go func() {
		wg.Wait()
		close(c)
	}()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, &reply)
			if !ok {
				c <- nil
				return
			}
			c <- &reply
		}(i)
	}
	grantedCount := 1
	notGrantedCount := 0
	tgt := rf.peersLen / 2
	for finish := 1; finish < rf.peersLen; finish++ {
		reply := <-c
		rf.mu.Lock()
		// 如果当前自己的任期号改变了, 说明进入了新的任期, 直接返回
		if rf.currentTerm != lcTerm {
			rf.mu.Unlock()
			break
		}

		// 如果状态不是CANDIDATE状态, 说明退化为FOLLOWER了, 直接返回
		if rf.identify != CANDIDATE {
			rf.mu.Unlock()
			break
		}

		rf.mu.Unlock()
		if reply == nil {
			notGrantedCount++
			continue
		}

		if reply.VoteGranted {
			grantedCount++
			if grantedCount > tgt {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.identify != LEADER {
						rf.identify = LEADER
						nextIndex := len(rf.logs)
						for i := range rf.nextIndex {
							rf.nextIndex[i] = nextIndex
						}
						// 同时发送HeartBeat信号(其实和AppendEntries一样, 都是一个接口)
						rf.doAppendCh <- HeartBeat
						// go rf.sendLogEntry(HeartBeat)
						DPrintf("%v: %v 赢得了选举，变为领导者，发送了初始心跳",
							args.Term, rf.me)
					}
				}()
				wonCh <- 1
				break
			}
		} else {
			// 如果当前任期小于返回的任期, 当前节点变为follower, 停止选举操作
			if args.Term < reply.Term {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentTerm < reply.Term {
						DPrintf("%v: %v 发送投票后发现自己过时，变回追随者", args.Term, rf.me)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.identify = FOLLOWER
						// 选举失败标志
						wonCh <- 1
					}
				}()
			}
			break
		}
		notGrantedCount++
		if notGrantedCount > tgt {
			break
		}
	}

}
