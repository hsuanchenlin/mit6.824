package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int)  int {
	if a > b {
		return a
	}
	return b
}

func NewRandDuration(minDuration time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % minDuration
	return time.Duration(minDuration + extra)
}