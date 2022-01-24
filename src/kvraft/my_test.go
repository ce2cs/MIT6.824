package kvraft

import (
	"6.824/models"
	"6.824/porcupine"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func MyGenericTest(t *testing.T, part string, nclients int, nservers int, unreliable bool, crash bool, partitions bool, maxraftstate int, randomkeys bool) {
	debugLog("clients number: %v", nclients)
	debugLog("server number: %v", nservers)
	title := "Test: "
	if unreliable {
		// the network drops RPC requests and replies.
		title = title + "unreliable net, "
	}
	if crash {
		// peers re-start, and thus persistence must work.
		title = title + "restarts, "
	}
	if partitions {
		// the network may partition
		title = title + "partitions, "
	}
	if maxraftstate != -1 {
		title = title + "snapshots, "
	}
	if randomkeys {
		title = title + "random keys, "
	}
	if nclients > 1 {
		title = title + "many clients"
	} else {
		title = title + "one client"
	}
	title = title + " (" + part + ")" // 3A or 3B

	cfg := make_config(t, nservers, unreliable, maxraftstate)
	defer cfg.cleanup()

	cfg.begin(title)
	opLog := &OpLog{}

	ck := cfg.makeClient(cfg.All())

	done_partitioner := int32(0)
	done_clients := int32(0)
	ch_partitioner := make(chan bool)
	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}
	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		atomic.StoreInt32(&done_clients, 0)
		atomic.StoreInt32(&done_partitioner, 0)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := "" // only used when not randomkeys
			if !randomkeys {
				Put(cfg, myck, strconv.Itoa(cli), last, opLog, cli)
				debugLog("put key: %v with value: %v from clerk %v to client %v", cli, last, myck.me, cli)
			}
			for atomic.LoadInt32(&done_clients) == 0 {
				var key string
				if randomkeys {
					key = strconv.Itoa(rand.Intn(nclients))
				} else {
					key = strconv.Itoa(cli)
				}
				nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
				if (rand.Int() % 1000) < 500 {
					// log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv, opLog, cli)
					debugLog("append key: %v with value: %v from clerk %v to client %v", key, nv, myck.me, cli)
					if !randomkeys {
						last = NextValue(last, nv)
					}
					j++
				} else if randomkeys && (rand.Int()%1000) < 100 {
					// we only do this when using random keys, because it would break the
					// check done after Get() operations
					Put(cfg, myck, key, nv, opLog, cli)
					debugLog("put key: %v with value: %v from clerk %v to client %v", key, nv, myck.me, cli)
					j++
				} else {
					// log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key, opLog, cli)
					debugLog("get key: %v from clerk %v to client %v, result is %v", key, myck.me, cli, v)
					// the following check only makes sense when we're not using random keys
					if !randomkeys && v != last {
						t.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			}
		})

		if partitions {
			// Allow the clients to perform some operations without interruption
			time.Sleep(1 * time.Second)
			go partitioner(t, cfg, ch_partitioner, &done_partitioner)
		}
		time.Sleep(5 * time.Second)

		atomic.StoreInt32(&done_clients, 1)     // tell clients to quit
		atomic.StoreInt32(&done_partitioner, 1) // tell partitioner to quit

		if partitions {
			// log.Printf("wait for partitioner\n")
			<-ch_partitioner
			// reconnect network and submit a request. A client may
			// have submitted a request in a minority.  That request
			// won't return until that server discovers a new term
			// has started.
			cfg.ConnectAll()
			// wait for a while so that we have a new term
			time.Sleep(electionTimeout)
		}

		if crash {
			// log.Printf("shutdown servers\n")
			for i := 0; i < nservers; i++ {
				cfg.ShutdownServer(i)
			}
			debugLog("shutdown all servers")
			// Wait for a while for servers to shutdown, since
			// shutdown isn't a real crash and isn't instantaneous
			time.Sleep(electionTimeout)
			// log.Printf("restart servers\n")
			// crash and re-start all
			for i := 0; i < nservers; i++ {
				cfg.StartServer(i)
			}
			debugLog("restart all servers")
			cfg.ConnectAll()
		}

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key, opLog, 0)
			debugLog("get key: %v from clerk %v to client %v, result is %v", key, ck.me, 0, v)
			if !randomkeys {
				checkClntAppends(t, i, v, j)
			}
		}

		if maxraftstate > 0 {
			// Check maximum after the servers have processed all client
			// requests and had time to checkpoint.
			sz := cfg.LogSize()
			if sz > 8*maxraftstate {
				t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
			}
		}
		if maxraftstate < 0 {
			// Check that snapshots are not used
			ssz := cfg.SnapshotSize()
			if ssz > 0 {
				t.Fatalf("snapshot too large (%v), should not be used when maxraftstate = %d", ssz, maxraftstate)
			}
		}
	}

	res, info := porcupine.CheckOperationsVerbose(models.KvModel, opLog.Read(), linearizabilityCheckTimeout)
	if res == porcupine.Illegal {
		file, err := ioutil.TempFile("", "*.html")
		if err != nil {
			fmt.Printf("info: failed to create temp file for visualization")
		} else {
			err = porcupine.Visualize(models.KvModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		t.Fatal("history is not linearizable")
	} else if res == porcupine.Unknown {
		fmt.Println("info: linearizability check timed out, assuming history is ok")
	}

	cfg.end()
}

func debugLog(fmtStr string, args ...interface{}) {
	fmt.Printf("----------------------------------------------------------\n")
	log.Printf("Test: "+fmtStr, args...)
	fmt.Printf("----------------------------------------------------------\n")
}

//func TestSnapshotUnreliable3BDIY(t *testing.T) {
//	// Test: unreliable net, snapshots, many clients (3B) ...
//	MyGenericTest(t, "3B", 5, 5, true, false, false, 1000, false)
//}
