package raft

import (
	"bytes"
	"labgob"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// Raft state
	wState := new(bytes.Buffer)
	eState := labgob.NewEncoder(wState)

	eState.Encode(rf.currentTerm)
	eState.Encode(rf.votedFor)
	for i := 1; i <= rf.lastLogIndex; i++ {
		eState.Encode(rf.logs[i])
	}
	dataState := wState.Bytes()

	// snapshot
	wSnapshot := new(bytes.Buffer)
	eSnapshot := labgob.NewEncoder(wSnapshot)
	if rf.snapshotIndex > 0 {
		eSnapshot.Encode(rf.snapshotTerm)
		eSnapshot.Encode(rf.snapshotIndex)
		eSnapshot.Encode(rf.snapshotData)
	}
	dataSnapshot := wSnapshot.Bytes()

	rf.persister.SaveStateAndSnapshot(dataState, dataSnapshot)
	rf.isPersist = false
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)

	var val = Entry{}
	for d.Decode(&val) == nil {
		rf.logs = append(rf.logs, val)
	}
	rf.lastLogIndex = len(rf.logs) - 1
}

func (rf *Raft) readSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.snapshotTerm)
	d.Decode(&rf.snapshotIndex)
	d.Decode(&rf.snapshotData)
}
