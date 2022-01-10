package raft

import (
	"log"
	"math/rand"
	"testing"
	"time"
)

//
//import (
//	"log"
//	"math/rand"
//	"testing"
//	"time"
//)
//
////
//// Test the scenarios described in Figure 8 of the extended Raft paper. Each
//// iteration asks a leader, if there is one, to insert a command in the Raft
//// log.  If there is a leader, that leader will fail quickly with a high
//// probability (perhaps without committing the command), or crash after a while
//// with low probability (most likey committing the command).  If the number of
//// alive servers isn't enough to form a majority, perhaps start a new server.
//// The leader in a new term may try to finish replicating log entries that
//// haven't been committed yet.
////
//func TestFigure82CWithLog(t *testing.T) {
//	commandIdx := 0
//	servers := 5
//	cfg := make_config(t, servers, false, false)
//	defer cfg.cleanup()
//
//	cfg.begin("Test (2C): Figure 8")
//
//	cfg.one(commandIdx, 1, true)
//	commandIdx += 1
//
//	nup := servers
//	for iters := 0; iters < 1000; iters++ {
//		leader := -1
//		for i := 0; i < servers; i++ {
//			if cfg.rafts[i] != nil {
//				_, _, ok := cfg.rafts[i].Start(commandIdx)
//				commandIdx += 1
//				if ok {
//					leader = i
//				}
//			}
//		}
//
//		if (rand.Int() % 1000) < 100 {
//			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
//			time.Sleep(time.Duration(ms) * time.Millisecond)
//			log.Printf("Test scripts: slept %v miliseconds", ms)
//		} else {
//			ms := (rand.Int63() % 13)
//			time.Sleep(time.Duration(ms) * time.Millisecond)
//			log.Printf("Test scripts: slept %v miliseconds", ms)
//		}
//
//		if leader != -1 {
//			cfg.crash1(leader)
//			log.Printf("Test scripts: server %v crashed", leader)
//			nup -= 1
//		}
//
//		if nup < 3 {
//			s := rand.Int() % servers
//			if cfg.rafts[s] == nil {
//				cfg.start1(s, cfg.applier)
//				cfg.connect(s)
//				log.Printf("Test scripts: server %v recovered", s)
//				nup += 1
//			}
//		}
//	}
//
//	for i := 0; i < servers; i++ {
//		if cfg.rafts[i] == nil {
//			cfg.start1(i, cfg.applier)
//			cfg.connect(i)
//		}
//	}
//	log.Printf("Test scripts: recovered all server")
//
//	cfg.one(commandIdx, servers, true)
//	commandIdx += 1
//
//	cfg.end()
//}
//
//func TestFigure8Unreliable2CWithLog(t *testing.T) {
//	commandIdx := 0
//	servers := 5
//	cfg := make_config(t, servers, true, false)
//	defer cfg.cleanup()
//
//	cfg.begin("Test (2C): Figure 8 (unreliable)")
//
//	cfg.one(commandIdx, 1, true)
//	commandIdx += 1
//
//	nup := servers
//	for iters := 0; iters < 1000; iters++ {
//		if iters == 200 {
//			cfg.setlongreordering(true)
//		}
//		leader := -1
//		for i := 0; i < servers; i++ {
//			_, _, ok := cfg.rafts[i].Start(rand.Int() % 10000)
//			if ok && cfg.connected[i] {
//				leader = i
//			}
//		}
//
//		if (rand.Int() % 1000) < 100 {
//			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
//			time.Sleep(time.Duration(ms) * time.Millisecond)
//		} else {
//			ms := (rand.Int63() % 13)
//			time.Sleep(time.Duration(ms) * time.Millisecond)
//		}
//
//		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
//			cfg.disconnect(leader)
//			nup -= 1
//		}
//
//		if nup < 3 {
//			s := rand.Int() % servers
//			if cfg.connected[s] == false {
//				cfg.connect(s)
//				nup += 1
//			}
//		}
//	}
//
//	for i := 0; i < servers; i++ {
//		if cfg.connected[i] == false {
//			cfg.connect(i)
//		}
//	}
//
//	cfg.one(rand.Int()%10000, servers, true)
//
//	cfg.end()
//}

func TestBackupSelfDefined(t *testing.T) {
	rand.Seed(0)
	commandIdx := 0
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): leader backs up quickly over incorrect follower logs")

	cfg.one(commandIdx, servers, true)
	commandIdx += 1

	// put leader and one follower in a partition
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	log.Printf("Test Scripts: disconnect 3 servers: %v, %v, %v",
		(leader1+2)%servers,
		(leader1+3)%servers,
		(leader1+4)%servers)

	startCommandIdx := commandIdx
	// submit lots of commands that won't commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(commandIdx)
		commandIdx += 1
	}

	log.Printf("Test Scripts: submitted commandIdx from %v to %v, supposed to not commit",
		startCommandIdx, commandIdx)

	time.Sleep(RaftElectionTimeout / 2)

	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)

	log.Printf("Test Scripts: disconnect 2 servers: %v, %v",
		(leader1+0)%servers,
		(leader1+1)%servers)

	// allow other partition to recover
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	log.Printf("Test Scripts: recovered 3 servers: %v, %v, %v",
		(leader1+2)%servers,
		(leader1+3)%servers,
		(leader1+4)%servers)

	// lots of successful commands to new group.
	startCommandIdx = commandIdx

	for i := 0; i < 50; i++ {
		cfg.one(commandIdx, 3, true)
		commandIdx += 1
	}

	log.Printf("Test Scripts: submitted commandIdx from %v to %v, supposed to succeed",
		startCommandIdx, commandIdx)

	// now another partitioned leader and one follower
	leader2 := cfg.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	cfg.disconnect(other)

	log.Printf("Test Scripts: disconnect 1 servers: %v", other)

	// lots more commands that won't commit

	startCommandIdx = commandIdx

	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(commandIdx)
		commandIdx += 1
	}

	log.Printf("Test Scripts: submitted commandIdx from %v to %v, supposed to not commit",
		startCommandIdx, commandIdx)

	time.Sleep(RaftElectionTimeout / 2)

	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	log.Printf("Test Scripts: disconnected all servers")

	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	log.Printf("Test Scripts: reconnected 3 servers: %v, %v, %v",
		(leader1+0)%servers,
		(leader1+1)%servers,
		other)

	// lots of successful commands to new group.
	startCommandIdx = commandIdx
	for i := 0; i < 50; i++ {
		cfg.one(commandIdx, 3, true)
		commandIdx += 1
	}

	log.Printf("Test Scripts: submitted commandIdx from %v to %v, supposed to succeed",
		startCommandIdx, commandIdx)

	// now everyone
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}

	log.Printf("Test Scripts: recover every server")

	cfg.one(commandIdx, servers, true)

	log.Printf("Test Scripts: add final command %v, supposed to succeed", commandIdx)
	cfg.end()
}
