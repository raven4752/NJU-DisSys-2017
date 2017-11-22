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
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

import "bytes"
import (
	"encoding/gob"
	"time"
	"math/rand"
	"fmt"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}
type Log struct {
	term    int
	Command interface{}
}
var poemOrder int=0


//
// A Go object implementing a single Raft peer.
//
const NOCANDIDATE int = -1
const VOTETIMEOUTBASIC int = 150
const HEARTBEATTIMEOUT int = 30
const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
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
	log         []Log
	//volatile state
	commitIndex    int
	lassApplied    int
	identification int
	electTimeOut<-   chan time.Time
	heartBeatTimeOut<-chan time.Time
	applyEntriesArgsChan chan AppendEntriesTuple
	AppendEntriesReplyChan chan AppendEntriesReply
	RequestVoteArgsChan chan RequestVoteTuple
	RequestVoteReplyChan chan RequestVoteReply
	//volatile state on leaders
	nextIndex  []int
	matchIndex []int
}


// return currentTerm and whether this server
// believes it is the leader.
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
func (rf *Raft) getIdentification()int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.identification
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	term int
}
type AppendEntriesReply struct {
	term int
	success bool
}
type RequestVoteTuple struct {
	request RequestVoteArgs
	reply *RequestVoteReply
}
type AppendEntriesTuple struct {
	request AppendEntriesArgs
	reply *AppendEntriesReply
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) resetElectTimeOut(){
	D:=time.Duration((rand.Intn(VOTETIMEOUTBASIC)+VOTETIMEOUTBASIC)*1000)
	rf.electTimeOut =time.After(D)
}
func (rf *Raft)resetHeartBeatTimeOut()  {
	D:=time.Duration(HEARTBEATTIMEOUT*1000)
	rf.heartBeatTimeOut=time.After(D)
}
func (rf *Raft) checkTerm(term int)bool {
	if term > rf.currentTerm {
		//reset current identification due to staled Term
		rf.currentTerm = term
		rf.identification = FOLLOWER
		rf.resetElectTimeOut()
		return true
		//reset idletime
	}
	return false
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs ,reply *AppendEntriesReply)  {
	rf.applyEntriesArgsChan<-AppendEntriesTuple{args,reply}
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.RequestVoteArgsChan<-RequestVoteTuple{args,reply}
}
func (rf *Raft) HandleAppendEntries(argsTuple AppendEntriesTuple) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args:=argsTuple.request
	reply:=argsTuple.reply
	term :=args.term
	rf.checkTerm(term)
	reply.term,reply.success=rf.currentTerm,term>=rf.currentTerm
}
func (rf *Raft) HandleRequestVote(argsTuple RequestVoteTuple) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args:=argsTuple.request
	reply:=argsTuple.reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	term, candidateId, lastLogIndex, lastLogTerm := args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm
	rf.checkTerm(term)
	if myLastLogIndex := len(rf.log); rf.identification == FOLLOWER &&
		term >= rf.currentTerm &&
		lastLogIndex >= myLastLogIndex &&(myLastLogIndex>=0|| lastLogTerm >= rf.log[myLastLogIndex-1].term ){
		//only reply vote when at FOLLOWER STATE
		//change my vote
		rf.votedFor = candidateId
		reply.VoteGranted = true
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int ,args AppendEntriesArgs,reply*AppendEntriesReply)bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf*Raft) startCampaign()bool  {
	rf.mu.Lock()
	//increase current term
	rf.currentTerm+=1
	rf.identification=CANDIDATE
	rf.votedFor=rf.me
	rf.resetElectTimeOut()
	rf.mu.Unlock()
	voteCount:=make(chan int,1)
	go func(){
		args:=RequestVoteArgs{rf.currentTerm,rf.me,len(rf.log),rf.log[len(rf.log)-1].term}
		count :=0
		for  i := 0; i<len(rf.peers);i++{
			reply:=&RequestVoteReply{}
			rf.sendRequestVote(i,args,reply)
			term:=reply.Term
			//rf.mu.Lock()
			staled:=rf.checkTerm(term)
			//rf.mu.Unlock()
			if staled{
				break
			}
			if reply.VoteGranted{
				count +=1
			}
		}
		voteCount<- count
	}()
	select {
	case count:= <- voteCount:
		//get all response in time
		if count>len(rf.peers)/2{
		//speak two poems
		fmt.Printf("%c", []rune("苟利国家生死以岂因祸福避趋之")[poemOrder%14])
		poemOrder++
		rf.mu.Lock()
		rf.identification=LEADER
		rf.mu.Unlock()
		return true
	}else {
		return false
	}
	case <-time.After(time.Microsecond*time.Duration(rf.electTimeOut)):
		//timeout,reset timeout and wait for new campaign
		rf.resetElectTimeOut()
		return false
	}


}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
func timestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
func (rt *Raft) selectAsLeader() {
	for  {
		select {
		case rt.heartBeatTimeOut://time to send heartbeat
			
			
		}
	}
}
func (rt *Raft)selectAsFollower()  {

}
func (rt *Raft) selectAsCandidate() {

}
func (rf*Raft)mainloop(applyCh chan ApplyMsg)  {
	//wait until time
	for{
		switch id:=rf.getIdentification() id{
		case LEADER:
			//send heartbeats when heartbeat timeout
			//check request/response received,if staled,convert to follower		
			rf.selectAsLeader()

		case FOLLOWER:
			rf.selectAsFollower()
			//become candidate and start campaign when election timeout
			//reset election timeout when receive leader heartbeat
		case CANDIDATE:
			//check request/response received,if staled,convert to follower
			//on all voted received :if over half ,convert to leader
			//if not,keep candidate
			//when election timeout start campaign
			rf.selectAsFollower()
		}
	}
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.identification=FOLLOWER
	//init electTimeOut
	rf.resetElectTimeOut()
	rf.heartBeatTimeOut=nil
	rf.votedFor=NOCANDIDATE
	//init log with a empty log to make index starting from 1
	rf.log=append(rf.log,Log{})
	//create buffer channel
	numpeer:=len(peers)
	rf.AppendEntriesReplyChan=make(chan AppendEntriesReply,numpeer)
	rf.applyEntriesArgsChan=make(chan AppendEntriesTuple)
	rf.RequestVoteArgsChan=make(chan RequestVoteTuple)
	rf.RequestVoteReplyChan=make(chan RequestVoteReply,numpeer)
	go rf.mainloop(applyCh)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
