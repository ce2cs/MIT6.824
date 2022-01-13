package raft

import (
	"log"
	"testing"
)

func snapcommonWithLog(t *testing.T, name string, disconnect bool, reliable bool, crash bool) {
	commandIdx := 0
	iters := 30
	servers := 3
	cfg := make_config(t, servers, !reliable, true)
	defer cfg.cleanup()

	cfg.begin(name)

	cfg.one(commandIdx, servers, true)
	commandIdx += 1
	leader1 := cfg.checkOneLeader()

	for i := 0; i < iters; i++ {
		victim := (leader1 + 1) % servers
		sender := leader1
		if i%3 == 1 {
			sender = (leader1 + 1) % servers
			victim = leader1
		}

		if disconnect {
			cfg.disconnect(victim)
			cfg.one(commandIdx, servers-1, true)
			commandIdx += 1
			log.Printf("Test: disconnect server %v", victim)
		}
		if crash {
			cfg.crash1(victim)
			cfg.one(commandIdx, servers-1, true)
			commandIdx += 1
			log.Printf("Test: crash server %v", victim)
		}
		// send enough to get a snapshot
		for i := 0; i < SnapShotInterval+1; i++ {
			cfg.rafts[sender].Start(commandIdx)
			commandIdx += 1
		}
		// let applier threads catch up with the Start()'s
		cfg.one(commandIdx, servers-1, true)
		commandIdx += 1

		if cfg.LogSize() >= MAXLOGSIZE {
			cfg.t.Fatalf("Log size too large")
		}
		if disconnect {
			// reconnect a follower, who maybe behind and
			// needs to rceive a snapshot to catch up.
			cfg.connect(victim)
			log.Printf("Test: reconnect server %v", victim)
			cfg.one(commandIdx, servers, true)
			commandIdx += 1
			leader1 = cfg.checkOneLeader()
		}
		if crash {
			cfg.start1(victim, cfg.applierSnap)
			log.Printf("Test: restart server %v", victim)
			cfg.connect(victim)
			cfg.one(commandIdx, servers, true)
			commandIdx += 1
			leader1 = cfg.checkOneLeader()
		}
	}
	cfg.end()
}

//func TestSnapshotBasic2DDIY(t *testing.T) {
//	snapcommonWithLog(t, "Test (2D): snapshots basic", false, true, false)
//}
//
//func TestSnapshotInstall2DDIY(t *testing.T) {
//	snapcommonWithLog(t, "Test (2D): install snapshots (disconnect)", true, true, false)
//}
//
//func TestSnapshotInstallUnreliable2DDIY(t *testing.T) {
//	snapcommonWithLog(t, "Test (2D): install snapshots (disconnect+unreliable)",
//		true, false, false)
//}
//
//func TestSnapshotInstallCrash2DIY(t *testing.T) {
//	snapcommonWithLog(t, "Test (2D): install snapshots (crash)", false, true, true)
//}
//
//func TestSnapshotInstallUnCrash2DDIY(t *testing.T) {
//	snapcommonWithLog(t, "Test (2D): install snapshots (unreliable+crash)", false, false, true)
//}
