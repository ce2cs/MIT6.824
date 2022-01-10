package raft

import (
	"log"
	"math"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Max(elements ...int) int {
	res := math.MinInt32
	for _, ele := range elements {
		if ele > res {
			res = ele
		}
	}
	return res
}

func Min(elements ...int) int {
	res := math.MaxInt32
	for _, ele := range elements {
		if ele < res {
			res = ele
		}
	}
	return res
}
