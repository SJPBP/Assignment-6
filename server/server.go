// server.go
// Prabhdeep Singh
// ps1282
package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
)

type Timestamp int64

type BeginArgs struct {
	TxID string
}

type BeginReply struct {
	Timestamp Timestamp
}

type ReadArgs struct {
	TxID string
	Key  string
}
type ReadReply struct {
	Value int
	Error string
}

type WriteArgs struct {
	TxID  string
	Key   string
	Value int
}
type WriteReply struct {
	Error string
}

type CommitArgs struct {
	TxID string
}
type CommitReply struct {
	Success bool
	Error   string
}

type TransactionService int64

type Transaction struct {
	Timestamp Timestamp
	TxID      string
	// Add channel
}

// Create map to store every transaction
var transaction = make(map[string]*Transaction)

// Struct for key used for transaction
type KEY struct {
	committed_value     int
	committed_timestamp int
	RTS                 []int
	TW                  map[Timestamp]int
}

// Create empty store map
// Store holding x and y variable
var store = make(map[string]*KEY)

func (s *TransactionService) BeginTransaction(args *BeginArgs, reply *BeginReply) error {
	// Get timestamp of current time
	ts := Timestamp(time.Now().UnixMicro())

	// Add it to reply
	reply.Timestamp = ts

	// Add the transaction
	transaction[args.TxID].Timestamp = ts

	return nil
}

func (s TransactionService) Read(args *ReadArgs, reply *ReadReply) error {
	tx, ok := transaction[args.TxID]
	if ok {
		key, ok := store[args.Key]
		if ok {
			// if tx Timestamp > key commited timestamp
			if tx.Timestamp > key.committed_timestamp {
				//
			}
		} else {
			reply.Error = "Key not found"
		}
	} else {
		reply.Error = "Transaction not found"
	}

	return nil
}

func (s *TransactionService) Write(args *WriteArgs, reply *WriteReply) error {
	return nil
}

func (s *TransactionService) Commit(args *CommitArgs, reply *CommitReply) error {
	return nil
}

func main() {
	// Functions aviable
	transaction := new(TransactionService)
	rpc.Register(transaction) // Register RPC service

	// Create store with x and y with value of 0
	store["x"] = &KEY{} // Create empty KEY
	store["x"].committed_value = 0
	store["x"].committed_timestamp = 0
	store["x"].RTS = append(store["x"].RTS, 0)
	store["x"].TW = make(map[Timestamp]int)

	store["y"] = &KEY{}
	store["y"].committed_value = 0
	store["y"].committed_timestamp = 0
	store["y"].RTS = append(store["y"].RTS, 0)
	store["y"].TW = make(map[Timestamp]int)

	listener, err := net.Listen("tcp", ":4000") // Create TCP listener on port 4000
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	defer listener.Close() // Close listener when main exits

	fmt.Println("Server listening on TCP port 1234")

	for { // Note: this is an infinite loop
		conn, err := listener.Accept() // Accept the next incoming call and
		if err != nil {                // return a connection
			fmt.Println("\nConnection error:", err)
			continue
		}
		go rpc.ServeConn(conn) // Serve RPC over the connection using a goroutine
	}
}
