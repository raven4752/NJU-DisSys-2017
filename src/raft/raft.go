package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is Leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft Peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

import "bytes"
import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"
)

//
// as each Raft Peer becomes aware that successive log Entries are
// committed, the Peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}
type Log struct {
	Term    int
	Command interface{}
}

var Verbose bool = false
var Info bool = true

//
// A Go object implementing a single Raft Peer.
//
const NOCANDIDATE int = -1
const VOTETIMEOUTBASIC int = 300
const HEARTBEATTIMEOUT int = 50
const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//persistent state
	currentTerm int
	votedFor    int

	log []Log
	//volatile state
	commitIndex      int //not used
	lassApplied      int //not used
	identification   int
	currentFollower  int //used in voting
	votesReceived    int //used in voting
	electTimeOut     <-chan time.Time
	heartBeatTimeOut <-chan time.Time

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int
	//start time
	start                  time.Time
	applyEntriesArgsChan   chan AppendEntriesTuple
	AppendEntriesReplyChan chan AppendEntriesReplyTuple
	RequestVoteArgsChan    chan RequestVoteTuple
	RequestVoteReplyChan   chan RequestVoteReply
	ApplyMsgChan           chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the Leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.identification == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC Reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term         int
	Leader       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term               int
	Success            bool
	ConflictEntryTerm  int
	ConflictEntryIndex int
}
type AppendEntriesReplyTuple struct {
	Reply AppendEntriesReply
	Peer  int
}
type RequestVoteTuple struct {
	Request   RequestVoteArgs
	ReplyChan chan RequestVoteReply
}
type AppendEntriesTuple struct {
	Request   AppendEntriesArgs
	ReplyChan chan AppendEntriesReply
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) resetElectTimeOut() {
	D := time.Duration((rand.Intn(VOTETIMEOUTBASIC) + VOTETIMEOUTBASIC) * int(time.Millisecond))
	//rf.print(fmt.Sprintf("resetting electionTimeOut to %d ms", D.Nanoseconds()/int64(time.Millisecond)))
	rf.electTimeOut = time.After(D)
}
func (rf *Raft) resetHeartBeatTimeOut() {
	D := time.Duration(HEARTBEATTIMEOUT * 1000 * 1000)
	rf.heartBeatTimeOut = time.After(D)
}
func (rf *Raft) checkTerm(term int) bool {
	if term > rf.currentTerm {
		rf.print(fmt.Sprintf("update to Term %d", term))
		//reset current identification due to staled Term
		rf.currentTerm = term
		if rf.identification != FOLLOWER {
			rf.identification = FOLLOWER
			rf.resetElectTimeOut()
			rf.currentFollower = 0
			rf.votesReceived = 0
			rf.heartBeatTimeOut = make(chan time.Time)
			rf.print("converted to followers")
		}
		return true
		//reset idletime
	}
	return false
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	replyChan := make(chan AppendEntriesReply)
	rf.applyEntriesArgsChan <- AppendEntriesTuple{args, replyChan}
	*reply = <-replyChan
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	replyChan := make(chan RequestVoteReply)
	rf.RequestVoteArgsChan <- RequestVoteTuple{args, replyChan}
	*reply = <-replyChan
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *Reply with RPC Reply, so caller should
// pass &Reply.
// the types of the args and Reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the Reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rt *Raft) print(msg string) {
	if Info {
		var state string
		switch rt.identification {
		case FOLLOWER:
			state = "follower"
		case CANDIDATE:
			state = "candidate"
		case LEADER:
			state = "Leader"
		default:
			state = "unknown"
		}
		timeElapsed := time.Now().Sub(rt.start)
		fmt.Printf("server %d T %d state %s : %s at %d \n", rt.me, rt.currentTerm, state, msg, timeElapsed.Nanoseconds()/(int64)(time.Millisecond))
	}
}
func (rt *Raft) verbose(msg string) {
	if Verbose {
		var state string
		switch rt.identification {
		case FOLLOWER:
			state = "follower"
		case CANDIDATE:
			state = "candidate"
		case LEADER:
			state = "Leader"
		default:
			state = "unknown"
		}
		timeElapsed := time.Now().Sub(rt.start)
		fmt.Printf("server %d T %d state %s : %s at %d \n", rt.me, rt.currentTerm, state, msg, timeElapsed.Nanoseconds()/(int64)(time.Millisecond))
	}
}
func (rf *Raft) isMyLogOld(term int, lastLogIndex int) bool {
	myLogTerm := rf.log[len(rf.log)-1].Term
	switch {
	case myLogTerm < term:
		return true
	case myLogTerm > term:
		return false
	case lastLogIndex >= len(rf.log)-1:
		return true
	default:
		return false

	}
}
func (rf *Raft) HandleRequestVote(argsTuple RequestVoteTuple) {
	args := argsTuple.Request
	reply := RequestVoteReply{}
	term, candidateId, logindex, logterm := args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm
	msg := fmt.Sprintf("receiving vote Request from %d", candidateId)
	rf.print(msg)
	reply.VoteGranted = false
	converted := rf.checkTerm(term)

	if rf.identification == FOLLOWER { //only Reply vote when at FOLLOWER state
		if rf.votedFor == candidateId {
			reply.VoteGranted = true
		} else if rf.isMyLogOld(logterm, logindex) {

			if converted {
				//change vote when  a newer candidate request votes

				rf.votedFor = candidateId
				msg := fmt.Sprintf("vote for %d", candidateId)
				rf.print(msg)
				reply.VoteGranted = true
			} else if term == rf.currentTerm && rf.votedFor == NOCANDIDATE {
				//change my vote when staled

				rf.votedFor = candidateId
				reply.VoteGranted = true
				msg := fmt.Sprintf("vote for %d", candidateId)
				rf.print(msg)
			}
		}

	} else {
		rf.print(fmt.Sprintf("i am not follower,ignore request"))
	}
	reply.Term = rf.currentTerm
	rf.print(fmt.Sprintf("my reply: %d", reply.VoteGranted))
	argsTuple.ReplyChan <- reply
}

func (rf *Raft) startCampaign() {
	//increase current Term

	rf.currentTerm += 1
	rf.identification = CANDIDATE
	rf.votedFor = rf.me
	rf.votesReceived = 0
	rf.currentFollower = 1
	rf.resetElectTimeOut()
	args := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
	rf.print("converted to candidate")
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(index int, replyChan chan RequestVoteReply) {
				rf.verbose(fmt.Sprintf("sending Request to %d", index))
				//Reply := <-rf.RequestVoteReplyChan
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(index, args, &reply)
				if ok {
					rf.verbose(fmt.Sprintf("rpc call Success %d", index))
				} else {
					//set Reply to Term to notify network fail
					reply.Term = rf.currentTerm
					rf.verbose(fmt.Sprintf("rpc call failed %d", index))
				}
				if reply.VoteGranted {
					rf.print(fmt.Sprintf("receive support %d", index))
				}
				replyChan <- reply
				//rf.print("write Reply to channel ")

			}(i, rf.RequestVoteReplyChan)
		}
	}

}
func (rf *Raft) HandleResponseVote(t RequestVoteReply) {
	//check Term
	rf.checkTerm(t.Term)

	if rf.identification == CANDIDATE {
		msg := fmt.Sprintf("handling Reply. current follower :%d", rf.currentFollower)
		rf.verbose(msg) //only react to voteReply when i am a candidate
		if t.Term == rf.currentTerm {
			rf.votesReceived += 1
			//check vote result
			if t.VoteGranted {
				rf.verbose("+1 supporter")
				rf.currentFollower += 1
			}
		} else {
			rf.verbose("outofDate vote Reply received")

		}
		if rf.currentFollower > len(rf.peers)/2 {
			//time to speak two poems

			//change identification
			rf.identification = LEADER

			//init indexes
			rf.initIndex()
			//clear ElectionTimeout
			rf.electTimeOut = make(chan time.Time)
			//send hearbeat immediately
			rf.heartBeat()
			rf.print(fmt.Sprintf(" the central cluster has made a decision"))
		} else {
			rf.verbose(fmt.Sprintf("i am not too modest ,how can i be a leader as a server"))

			if rf.votesReceived == len(rf.peers)-1 {
				rf.print(fmt.Sprintf("i think i should apply for professor"))
				//campaign failed,wait
				rf.resetElectTimeOut()
				//kill all sones
			}
		}
	}
}
func (rf *Raft) initIndex() {
	//init nextindex and match index
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.print(fmt.Sprintf("i am killed"))
	// Your code here, if desired.
}

func (rf *Raft) heartBeat() {
	log := rf.log[len(rf.log)-1]
	args := AppendEntriesArgs{rf.currentTerm, rf.me, len(rf.log) - 1, log.Term, []Log{}, rf.commitIndex}
	for i := 0; i < len(rf.peers); i++ {

		//if i am Leader ,send heartbeat
		go func(index int) {
			//create applyentriesArgs
			reply := &AppendEntriesReply{}

			rf.verbose(fmt.Sprintf("sending heartbeat to %d", index))

			ok := rf.sendAppendEntries(index, args, reply)
			if !ok {
				rf.verbose("heartbeat failed.")
			} else {
				replyPeer := AppendEntriesReplyTuple{*reply, index}

				rf.AppendEntriesReplyChan <- replyPeer
			}
		}(i)

	}
	//reset heartbeatTimeOut
	rf.resetHeartBeatTimeOut()
}

func (rf *Raft) HandleApplyEntries(t AppendEntriesTuple) {
	if len(t.Request.Entries) == 0 {
		rf.verbose("checking according to heartbeat")

	} else {
		rf.verbose("receiving appendentries call")
	}
	reply := AppendEntriesReply{}
	term := t.Request.Term
	index := t.Request.PrevLogIndex
	logterm := t.Request.PrevLogTerm
	entries := t.Request.Entries
	leaderCommit := t.Request.LeaderCommit
	reply.Success = true
	converted := rf.checkTerm(term)
	if rf.identification == FOLLOWER && !converted { //cancel election plan
		rf.resetElectTimeOut()
	}

	switch {
	case term < rf.currentTerm: //if term is old,return false
		reply.Success = false

		reply.Term = rf.currentTerm
		rf.print("rejected due to low Term")

	case len(rf.log)-1 < index: //not having that many indexes
		reply.Success = false
		//requesting for logs starting from the end
		reply.ConflictEntryIndex = len(rf.log)
		reply.ConflictEntryTerm = -1 //how to set the term???
		rf.print(fmt.Sprintf("index %d too big.request  logs from %d", index, reply.ConflictEntryIndex))

	case rf.log[index].Term != logterm: //log not matches
		reply.Success = false
		reply.ConflictEntryIndex = index
		reply.ConflictEntryTerm = rf.log[index].Term
		//for i := index; i >= 0; i-- {
		//	if rf.log[i-1].Term != rf.log[index].Term {
		//		reply.ConflictEntryIndex = i
		//		break
		//	}
		//}
		if index == 0 {
			panic("")
		}
		rf.print(fmt.Sprintf("log term not match for index %d,term %d,my term %d .request logs from %d", index, logterm, rf.log[index].Term, reply.ConflictEntryIndex))
	default: //update logs

		if len(entries) > 0 && rf.identification != LEADER { //a leader never overwrite it's log

			for i := index + 1; i < len(rf.log); i++ {
				if rf.log[i-1].Term != entries[i-index-1].Term {
					//pop all log starting from i and break
					rf.log = rf.log[0:i]
					break
				}
			}
			for i := 0; i < len(entries); i++ {
				//check if already have
				if len(rf.log)-1 >= index+1+i {

					rf.log[index+1+i] = entries[i]

				} else {
					rf.log = append(rf.log, entries[i])
				}
			}
			//rf.log = append(rf.log, entries...) //sweet!
			//update commit index
			rf.print(fmt.Sprintf("log appended current index: %d", len(rf.log)-1))
		}
		//modify the origin raft to deal with leader log added after send appendEntries ,before the follower reply the RPC
		if leaderCommit > rf.commitIndex {
			if leaderCommit > len(rf.log)-1 {
				rf.commitIndex = len(rf.log) - 1
			} else {
				rf.commitIndex = leaderCommit
			}
			rf.print(fmt.Sprintf("update commit index to %d", rf.commitIndex))

		}
		reply.ConflictEntryIndex = len(rf.log) - 1

	}
	reply.Term = rf.currentTerm

	t.ReplyChan <- reply
}
func (rf *Raft) HandleResponseApplyEntries(t AppendEntriesReplyTuple) {
	rf.checkTerm(t.Reply.Term)
	if rf.identification == LEADER {

		if !t.Reply.Success {

			index2send := t.Reply.ConflictEntryIndex

			if index2send > 0 { //fail due to log conflict
				// decrease nextindex
				if index2send < rf.nextIndex[t.Peer] {
					rf.nextIndex[t.Peer] = index2send
				}
				conflictTerm := t.Reply.ConflictEntryTerm
				var start int

				if len(rf.log)-1 < rf.nextIndex[t.Peer] {
					start = len(rf.log) - 1
				} else {
					start = rf.nextIndex[t.Peer]
				}

				index2send = start
				for i := start; i >= 1; i-- { //skip all log with the same conflict term
					if rf.log[i-1].Term <= 2*conflictTerm && rf.log[i-1].Term >= conflictTerm/2 {
						index2send = i
						break
					}
				}
				//index2send = 1
				entries := rf.log[index2send:]

				args := AppendEntriesArgs{rf.currentTerm, rf.me, index2send - 1, rf.log[index2send-1].Term, entries, rf.commitIndex}
				go func(args AppendEntriesArgs) {
					reply := &AppendEntriesReply{}

					rf.print(fmt.Sprintf("sending logs starting from %d to help Peer %d updating", index2send, t.Peer))
					ok := rf.sendAppendEntries(t.Peer, args, reply)
					if !ok {
						rf.print("sending log  failed.")
					} else {
						replyPeer := AppendEntriesReplyTuple{*reply, t.Peer}

						rf.AppendEntriesReplyChan <- replyPeer
					}
				}(args)
			}
		} else {
			if rf.matchIndex[t.Peer] != len(rf.log)-1 {
				rf.print(fmt.Sprintf("updating Peer %d 's match index to %d", t.Peer, len(rf.log)-1))

			}

			//update next index for Peer
			rf.nextIndex[t.Peer] = t.Reply.ConflictEntryIndex + 1 //len(rf.log)
			rf.matchIndex[t.Peer] = t.Reply.ConflictEntryIndex    //?
			//update match index for Peer
		}
	}

}

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the Leader.
//
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the Leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the Leader
// may fail or lose an election.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.identification == LEADER
	rf.verbose("receive client call")

	if !isLeader {
		return index, term, isLeader
	} else {
		newlog := Log{rf.currentTerm, command}
		//add log to local log
		rf.log = append(rf.log, newlog)

		rf.print(fmt.Sprintf("**********return to client with index %d***********", index))
		return index, term, isLeader

	}
}

func (rf *Raft) checkLog() {
	if rf.identification == LEADER {
		//check agreement made

		for i := rf.commitIndex + 1; i < len(rf.log); i++ {
			numagree := 0
			if rf.log[i].Term != rf.currentTerm {
				rf.verbose(fmt.Sprintf("rejected commit index to be %d due to Term %d", i, rf.log[i].Term))
				continue
			}
			for j := 0; j < len(rf.peers); j++ {
				if rf.matchIndex[j] >= i {
					numagree += 1
				}
			}

			if numagree > len(rf.peers)/2 {
				rf.commitIndex = i
				rf.print(fmt.Sprintf("update commit index to %d", rf.commitIndex))
			}
		}
	}
	for rf.commitIndex > rf.lassApplied {
		rf.lassApplied += 1

		rf.ApplyMsgChan <- ApplyMsg{rf.lassApplied, rf.log[rf.lassApplied].Command, false, []byte{}}

		rf.print(fmt.Sprintf("apply command  %d at  %d term %d", rf.log[rf.lassApplied].Command, rf.lassApplied, rf.log[rf.lassApplied].Term))

	}
}
func (rf *Raft) mainloop() {
	//wait until time
	for {
		select {
		case <-rf.heartBeatTimeOut: //send heartbeart if is Leader

			rf.verbose("time to heartbeat")

			rf.mu.Lock()
			rf.checkLog()
			if rf.identification == LEADER {
				rf.heartBeat()
			}
			rf.mu.Unlock()
		case t := <-rf.AppendEntriesReplyChan:

			rf.mu.Lock()
			rf.checkLog()
			rf.verbose(fmt.Sprintf("receiving heartbeat Reply Term : %d ", t.Reply.Term))
			rf.HandleResponseApplyEntries(t)
			rf.mu.Unlock()
		case t := <-rf.applyEntriesArgsChan: //receive apply Entries rpc

			rf.mu.Lock()

			rf.checkLog()
			rf.HandleApplyEntries(t)
			rf.persist() //persist before respond to rpc

			rf.mu.Unlock()
		case t := <-rf.RequestVoteArgsChan: //
			rf.mu.Lock()

			rf.checkLog()
			rf.HandleRequestVote(t)
			rf.persist() //persist before respond to rpc

			rf.mu.Unlock()
			//check his log if ia m leader

		case t := <-rf.RequestVoteReplyChan: //receive num voters
			rf.mu.Lock()
			rf.checkLog()
			rf.HandleResponseVote(t)
			rf.mu.Unlock()
		case <-rf.electTimeOut: //start campaign when i am a follower or candidate
			rf.mu.Lock()
			rf.checkLog()
			if rf.identification == FOLLOWER || rf.identification == CANDIDATE {
				rf.startCampaign()
			}
			rf.mu.Unlock()

		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.start = time.Now()
	// Your initialization code here.
	rf.identification = FOLLOWER
	rf.votedFor = NOCANDIDATE
	//init log with a empty log to make index starting from 1
	rf.log = append(rf.log, Log{})

	//init electTimeOut
	rf.resetElectTimeOut()
	//init heartBeatTimeOut
	rf.heartBeatTimeOut = make(chan time.Time)
	//create buffer channel
	numpeer := len(peers)
	rf.AppendEntriesReplyChan = make(chan AppendEntriesReplyTuple, numpeer)
	rf.applyEntriesArgsChan = make(chan AppendEntriesTuple)
	rf.RequestVoteArgsChan = make(chan RequestVoteTuple)
	rf.RequestVoteReplyChan = make(chan RequestVoteReply, numpeer)
	rf.ApplyMsgChan = applyCh
	//rf.CancelCampaignChan = make(chan int)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.print("i am alive")
	go rf.mainloop()

	return rf
}
