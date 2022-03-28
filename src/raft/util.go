package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = true

var isInit = 2

func DPrintf(format string, a ...interface{}) {
	if isInit == 1 {
		logFile, err := os.OpenFile("./log.log", os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			panic(err)
		}
		log.SetOutput(logFile)
		isInit = 2
	}

	if Debug {
		log.Printf(format, a...)
	}
	return
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
