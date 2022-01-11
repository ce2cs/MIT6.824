package raft

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func internalChurnWithLog(t *testing.T, unreliable bool) {

	servers := 5
	commandLock := sync.Mutex{}
	commandIdx := 0
	cfg := make_config(t, servers, unreliable, false)
	defer cfg.cleanup()

	if unreliable {
		cfg.begin("Test (2C): unreliable churn")
	} else {
		cfg.begin("Test (2C): churn")
	}

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() {
			log.Printf("Test: client %v adding %v to channel", me, ret)
			ch <- ret
		}()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			commandLock.Lock()
			x := commandIdx
			commandIdx += 1
			commandLock.Unlock()
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a leader
				cfg.mu.Lock()
				rf := cfg.rafts[i]
				cfg.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Start(x)
					if ok1 {
						log.Printf("Test: client %v submit command %v to server %v succeed", me, x, i)
						ok = ok1
						index = index1
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := cfg.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
								log.Printf("Test: client %v added command %v to buffer, now: %v", me, x, values)
							}
						} else {
							cfg.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
					log.Printf("Test: client %v wait %v miliseconds", me, to)
				}
			} else {
				log.Printf("Test: client %v wait %v miliseconds", me, 79+me*17)
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			cfg.disconnect(i)
			log.Printf("Test: server %v disconnected at iteration %v", i, iters)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if cfg.rafts[i] == nil {
				cfg.start1(i, cfg.applier)
			}
			cfg.connect(i)
			log.Printf("Test: server %v reconnected at iteration %v", i, iters)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if cfg.rafts[i] != nil {
				cfg.crash1(i)
			}
			log.Printf("Test: server %v crashed at iteration %v", i, iters)
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	cfg.setunreliable(false)
	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i, cfg.applier)
		}
		cfg.connect(i)
	}

	log.Printf("Test: reconnect all servers")

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := cfg.one(commandIdx, servers, true)
	log.Printf("send last command %v to all servers", commandIdx)
	commandIdx += 1

	really := make([]int, lastIndex+1)
	for index := 1; index <= lastIndex; index++ {
		v := cfg.wait(index, servers, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	log.Printf("compare values :%v, really %v", values, really)
	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			cfg.t.Fatalf("didn't find a value")
		}
	}

	cfg.end()
}
