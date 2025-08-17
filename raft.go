package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"sync"
	"sync/atomic"
	"time"
	"cs351/labrpc"
	"math/rand"
	"fmt"
	"strings"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Entries []interface{}
	Terms []int
}

type State int 
const (
    Follower  State = iota 
    Candidate                  
    Leader                    
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // This peer's index into peers[] **WILL TREAT THIS AS CANDIDATE ID** 
	dead  int32               // Set by Kill()

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	// Persistent state on all servers
	State State 				// what state the server is in
	HeardLast time.Time 		// this is the last time the server heard from a leader
	RandomTimeout time.Duration // election timeout length
	CurrentTerm int				// The current term of a server
	VotedFor int				// **MIGHT NEED TO CHANGE** the index of peer that this server voted for 
	Log Log 
	applyCh chan ApplyMsg

	// Volatile State on all servers
	CommitIndex int 			// Index of highest log entry known to be committed
	LastApplied int				// Index of highest log entry applied to state machine
	
	
	// Volatile state for leaders 
	NextIndex []int 			// Index of next log entry to send to that server for each server
	MatchIndex []int 			// Index of highest log entry known to be replicated on that server for each server
	newCommand chan struct{} 	// detects when a new command comes in 
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	// Your code here (3A).
	term = rf.CurrentTerm 
	if rf.State == Leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int 			// candidates term
	CandidateId int 	// candidate requesting vote
	LastLogIndex int 	// index of candidates last log entry 
	LastLogTerm int 	// term of candidates last log entry
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int 			// currentTerm for candidate to update itself
	VoteGranted bool		// true means candidate receieved vote 
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	// rf is the Voter and args is the Candidate
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if args.Term < rf.CurrentTerm {
		return
	} else if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.State = Follower
		rf.VotedFor = -1
	} 
	


	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		lastIndex := len(rf.Log.Entries) 
		lastTerm := 0
		if lastIndex > 0 {
			lastTerm = rf.Log.Terms[lastIndex-1]
		}


		if (args.LastLogTerm > lastTerm) || 
			(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex) {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.HeardLast = time.Now()
		} 
	}
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.
// ORIGINAL FUNCTION
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }
// Going to modify this function so it works with our future implementation
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != Leader {
		return -1, rf.CurrentTerm, false
	}
	// Your code here (3B).
	rf.Log.Entries = append(rf.Log.Entries, command)
	rf.Log.Terms = append(rf.Log.Terms, rf.CurrentTerm)
	index := len(rf.Log.Entries)
	
	rf.MatchIndex[rf.me] = index
	rf.NextIndex[rf.me] = index + 1

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendNewCommand(i)
		}
	}

	return index, rf.CurrentTerm, true
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


// AppendEntries 
type AppendEntriesArgs struct {
	Term int 			// leaders term
	LeaderId int 		// so followers can redirect clients
	PrevLogIndex int	// index of log entry immediately preceding new ones 
	PrevLogTerm int		// term of PrevLogIndex entry 
	Entries Log 		// **WILL HAVE TO FIGURE OUT CORRECT TYPE FOR 3B 
	// log entries to store (empty for heartbeat; may send more than one for efficiency) 
	LeaderCommit int 	// leader's commitIndex
}

type AppendEntriesReply struct{
	Term int 			// currentTerm, for leader to update itself
	Success bool 		// true if follower contained entry matching PrevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//if !(len(args.Entries.Entries) == 0) {
		// loggging code 
	//log.Printf("Server %d (T%d %d) received AppendEntries from %d: prevIndex=%d prevTerm=%d entries=%v commitIndex=%d | My log: %s",
	//	rf.me, rf.CurrentTerm, rf.State, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, 
	//len(args.Entries.Entries), rf.CommitIndex, rf.logString())
	//}


	// since the receiving server gets an append entries, we have to reset the timeout
	reply.Term = rf.CurrentTerm
	reply.Success = false




	if args.Term < rf.CurrentTerm {
		//log.Printf("AE Case 1")
		return
	}

	rf.HeardLast = time.Now() 

	// here rf is the Receiver/Follower and args is the Leader
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.State = Follower

		//log.Printf("AE Case 2")
		// not sure to add this rf.State = Follower
	}



	if args.PrevLogIndex > len(rf.Log.Entries) {
		//log.Printf("AE Case 3")
		return
	}

	if args.PrevLogIndex > 0 {
		 if (args.PrevLogIndex-1 >= len(rf.Log.Terms) || rf.Log.Terms[args.PrevLogIndex-1] != args.PrevLogTerm) {
			//log.Printf("AE Case 4")
			return 
		}
	}

	
	// append to the followers log based on nextindex

	// for rule 3 and 4 conceptually: we can decrement until we find the last entry where the indices are the same and the term agrees,
	// then from there on the followers log is replaced by the leaders log
	i := 0
	for ; i < len(args.Entries.Entries); i++ {
		conflictingIndex := args.PrevLogIndex + i
		if conflictingIndex < len(rf.Log.Entries) {
			if rf.Log.Terms[conflictingIndex] != args.Entries.Terms[i] {
				rf.Log.Entries = rf.Log.Entries[:conflictingIndex]
				rf.Log.Terms = rf.Log.Terms[:conflictingIndex]
				break
			}
		} else {
			break
		}
	}

	if i < len(args.Entries.Entries) {
		rf.Log.Entries = append(rf.Log.Entries, args.Entries.Entries[i:]...)
		rf.Log.Terms = append(rf.Log.Terms, args.Entries.Terms[i:]...)
	}

	// rule 5
    if args.LeaderCommit > rf.CommitIndex {
		//log.Printf("AE Case 6")
        rf.CommitIndex = min(args.LeaderCommit, len(rf.Log.Entries))
    }

    reply.Success = true

	//log.Printf("Server %d (T%d) reply Success=%v | New log: %s CommitIndex=%d",
    //rf.me, rf.CurrentTerm, reply.Success, rf.logString(), rf.CommitIndex)


}



// generates a random timeout for each server that calls it 
func (rf *Raft) generateTimeout() {
	// does this need a lock?
	randint := rand.Intn(251) + 500 // random timeout between 500-750ms
	rf.RandomTimeout = time.Duration(randint) * time.Millisecond
}


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 10)

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.State == Leader {
			rf.mu.Unlock()
			continue
		}
		
		if time.Since(rf.HeardLast) > rf.RandomTimeout { // check that the follower condition is correct
			go rf.startElection() // have to make sure that if they lose the election (i.e. receive a heartbeat)
			// that it converts back to a follower and ends the election
			// should be fine since receiving an append entries will convert them to a follower and 
			// they will automatically lose the election because other servers will advance to a later term
			// so the question is should this be run as a goroutine? I'd say probably but lets see 
		}
		rf.mu.Unlock()
	}	
}

// Election Infrastructure:
// First we define Futures and the Wait() function 
type Future struct {
	result chan interface{}
}

func NewFuture() *Future {
	return &Future{
		result: make(chan interface{}, 1),
	}
}

func (f *Future) CompleteFuture(res interface{}) {
	f.result <- res
	f.CloseFuture()
}

func (f *Future) GetResult() interface{} {
	return <-f.result
}

func (f *Future) CloseFuture() {
	close(f.result)
}

// Wait waits for the first n futures to return or for the timeout to expire,
// whichever happens first.
func Wait(futures []*Future, n int, timeout time.Duration, filter func(interface{}) bool) []interface{} {
	// TODO: Your code here

	length := len(futures)
	data_ch := make(chan interface{}, length)
	// spawning len(futures) go routines that wait for a value from the future, once we get that value its sent to data_ch
	for i := 0; i < length; i++ {
		go func(i int) {
			val := futures[i].GetResult()
			if filter(val) {
				data_ch <- val
			}
		}(i)
	}
	var result []interface{}
	count := 0
	timeout_ch := time.After(timeout)

	for count < n {
		select {
		case val := <- data_ch:
			result = append(result, val)
			count++
		case <- timeout_ch:
			return result
		}
	}
	return result
}

func electionFilter(res interface{}) bool {
	// has to send true if the response is true 
	val, ok := res.(RequestVoteReply);
	if ok && val.VoteGranted {
		return true
	} else {
		return false
	}
}

// Now we have the actual logic to start an election 
func (rf *Raft) startElection() {

	rf.mu.Lock()
	if rf.State == Leader {
		rf.mu.Unlock()
		return
	}

	rf.State = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.HeardLast = time.Now()

	lastLogIndex := len(rf.Log.Entries)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.Log.Terms[lastLogIndex-1]
	}

	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votes := 1
	voteChan := make(chan bool, len(rf.peers)-1)

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, &args, &reply)
				if ok {
					voteChan <- reply.VoteGranted
					if reply.Term > args.Term {
						rf.mu.Lock()
						if reply.Term > rf.CurrentTerm {
							rf.CurrentTerm = reply.Term
							rf.State = Follower
							rf.VotedFor = -1
						}
						rf.mu.Unlock()
					}
				} else {
					voteChan <- false
				}
			}(i)
		}
	}
	rf.mu.Unlock()

	// Count votes
	for i := 0; i < len(rf.peers)-1; i++ {
		if <-voteChan {
			votes++
			if votes > len(rf.peers)/2 {
				rf.mu.Lock()
				if rf.State == Candidate && rf.CurrentTerm == args.Term {
					rf.State = Leader
					for i := range rf.peers {
						rf.NextIndex[i] = len(rf.Log.Entries) + 1
						rf.MatchIndex[i] = 0
					}
					rf.MatchIndex[rf.me] = len(rf.Log.Entries)
					rf.mu.Unlock()
					go rf.becomeLeader()
					return
				}
				rf.mu.Unlock()
			}
		}
	}
}



// new function so we don't have to rely on channels, updates when we get a new command from start 
func (rf *Raft) sendNewCommand(follower int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.State != Leader {
		return
	}

	var args AppendEntriesArgs	
	majority := len(rf.peers)/2 
	// log.Printf("Leader %d (T%d) replicating log. NextIndex=%v MatchIndex=%v | Log: %s",
	// rf.me, rf.CurrentTerm, rf.NextIndex, rf.MatchIndex, rf.logString())

	var prevlogterm int
	prevlogindex := rf.NextIndex[follower] - 1
	if prevlogindex > 0 {
		if prevlogindex-1 < len(rf.Log.Terms) {
			prevlogterm = rf.Log.Terms[prevlogindex-1]
		} else {
			return
		}
	}

	var entries Log
	if rf.NextIndex[follower] <= len(rf.Log.Entries) {
		entries = Log{
			Entries: rf.Log.Entries[rf.NextIndex[follower]-1:],
			Terms:   rf.Log.Terms[rf.NextIndex[follower]-1:],
			}
	} else {
		entries = Log{
			Entries:  make([]interface{}, 0),
			Terms: make([]int, 0),
		}
	}

	args = AppendEntriesArgs{
		Term: rf.CurrentTerm,
		LeaderId: rf.me,
		PrevLogIndex: prevlogindex,
		PrevLogTerm: prevlogterm,
		Entries: Log{
			Entries: entries.Entries,
			Terms:  entries.Terms,
			}, 
		LeaderCommit: rf.CommitIndex,
		}

	

	go func() {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(follower, &args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.State = Follower
				rf.VotedFor = -1
				return
			}

			if rf.State != Leader || rf.CurrentTerm != args.Term {
				return
			}

			if reply.Success {
				newMatchIndex := args.PrevLogIndex + len(args.Entries.Entries)
				if newMatchIndex > rf.MatchIndex[follower] {
					rf.MatchIndex[follower] = newMatchIndex
					rf.NextIndex[follower] = newMatchIndex + 1
				}


				if rf.State == Leader {
					for N := len(rf.Log.Entries); N > rf.CommitIndex; N-- {
						count := 1 
						for i := range rf.peers {
							if i != rf.me && rf.MatchIndex[i] >= N {
								count++
							}
						}

						if count > majority && N > 0 && rf.Log.Terms[N-1] == rf.CurrentTerm {
							rf.CommitIndex = N
							break
						}
					}
				}
			} else {
				rf.NextIndex[follower] = max(1, rf.NextIndex[follower]-1)
				go rf.sendNewCommand(follower)
			}
		}
	}()

	}



func (rf *Raft) becomeLeader() {
	for !rf.killed() {
		// according to lec 9 slide 30, leader has to choose a new random timeout 
		rf.mu.Lock()
		if rf.State != Leader { // make sure it didn't get switched to another state in the time it took to get here
			rf.mu.Unlock()
			return
		}
		electionTerm := rf.CurrentTerm
		rf.mu.Unlock()
		
			// new for loop to send heartbeats to each server 
			for i := range rf.peers {
				if i != rf.me {
					go func (follower int) {
						rf.mu.Lock()
						// if the another server becomes leader, or if its not on the term it was elected for, recognise that and step down
						if rf.State != Leader || rf.CurrentTerm != electionTerm { 
							rf.mu.Unlock()										  
							return
						}

						var args AppendEntriesArgs
						//log.Printf("Leader sending Heartbeat | NextIndex: %v", rf.NextIndex)
						// set args for append entries
						if rf.NextIndex[follower] > len(rf.Log.Entries) {
							prevlogindex := rf.NextIndex[follower] - 1
							prevlogterm := 0
							if prevlogindex > 0 {
								if prevlogindex-1 < len(rf.Log.Terms) {
									prevlogterm = rf.Log.Terms[prevlogindex-1]
								} else {
									rf.mu.Unlock()
									return
								}
							}
							args = AppendEntriesArgs{
								Term: electionTerm,
								LeaderId: rf.me,
								PrevLogIndex: prevlogindex,
								PrevLogTerm: prevlogterm,
								Entries: Log{
									Entries: make([]interface{}, 0),
									Terms:   make([]int, 0),
									}, 
								LeaderCommit: rf.CommitIndex,
							}
							rf.mu.Unlock()
							
							reply := AppendEntriesReply{}
							rf.sendAppendEntries(follower, &args, &reply)

							// just handle the case where reply fails bc term is higher
							if reply.Term > electionTerm {
								rf.mu.Lock()
								if reply.Term > rf.CurrentTerm {
									rf.CurrentTerm = reply.Term
									rf.State = Follower
									rf.VotedFor = -1
								}
								rf.mu.Unlock()
							}
							return
						}
						rf.mu.Unlock()

					} (i)
				}
			}
			time.Sleep(100 * time.Millisecond)

		}
}

// leader helper functions 


	func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
		// this is where appendentries are sent 
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		return ok
	}

	func (rf *Raft) apply() {
		for !rf.killed() {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			for rf.CommitIndex > rf.LastApplied {
				rf.LastApplied++
				applyMessage := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log.Entries[rf.LastApplied-1],
				CommandIndex: rf.LastApplied, 
				}
				rf.mu.Unlock()
				//log.Printf("Server %d applying index %d: %v", 
                //rf.me, rf.LastApplied, applyMessage.Command)
				rf.applyCh <- applyMessage
				rf.mu.Lock()
			}
			rf.mu.Unlock()
			
		}
	}
	


// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	// Your initialization code here (3A, 3B).
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.generateTimeout()
	rf.State = Follower
	rf.HeardLast = time.Now()
	rf.VotedFor = -1
	rf.Log = Log{
		Entries: make([]interface{}, 0),
		Terms:   make([]int, 0),
	}         
	rf.applyCh = applyCh
	rf.LastApplied = 0
	rf.CommitIndex = 0 
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	// start ticker goroutine to start elections.
	go rf.ticker()
	go rf.apply()


	return rf
}



// logging method 
func (rf *Raft) logString() string {
    var entries []string
    for i, entry := range rf.Log.Entries {
        entries = append(entries, fmt.Sprintf("%d:%v(%d)", i, entry, rf.Log.Terms[i]))
    }
    return fmt.Sprintf("[%s]", strings.Join(entries, " "))
}
