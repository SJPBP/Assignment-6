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

// Struct for key used for transaction
type KEY struct {
	key                 string
	committed_value     int
	committed_timestamp int
	RTS                 []int
	TW                  map[int]int
}

// Create empty store map
// Store holding x and y variable
var store = make(map[string]KEY)

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
	return nil
}

func (s *TransactionService) Write(args *WriteArgs, reply *WriteReply) error {
	return nil
}

func (s *TransactionService) Commit(args *CommitArgs, reply *CommitReply) error {
	return nil
}

// type Backend struct {
// 	int
// 	mu   sync.Mutex
// 	rwMu sync.RWMutex
// }

// func (t *Backend) Put(args *PutArgs, reply *PutReply) error {
// 	t.mu.Lock()
// 	store[args.Key] = args.Value
//
// 	_, ok := store[args.Key]
//
// 	if ok == true {
// 		fmt.Printf("\nClient %v: put(%v,%v)\n", args.ClientName, args.Key, args.Value)
// 		printStore()
//
// 		t.mu.Unlock()
// 		return nil
// 	}
// 	t.mu.Unlock()
//
// 	return errors.New("Please provide key and value")
// }
//
// func (t *Backend) Get(args *GetArgs, reply *GetReply) error {
// 	t.rwMu.RLock()
// 	_, ok := store[args.Key]
//
// 	if ok == true {
// 		reply.Value = store[args.Key]
//
// 		fmt.Printf("\nClient %v: get(%v) = %v\n", args.ClientName, args.Key, reply.Value)
//
// 		t.rwMu.RUnlock()
// 		return nil
// 	}
// 	fmt.Printf("\nClient %v: get(%v): key not found\n", args.ClientName, args.Key)
//
// 	errorMessage := fmt.Sprintf("get(%v): key not found", args.Key)
//
// 	t.rwMu.RUnlock()
// 	return errors.New(errorMessage)
// }
//
// func (t *Backend) Del(args *DelArgs, reply *DelReply) error {
// 	t.mu.Lock()
// 	_, ok := store[args.Key]
//
// 	if ok == true {
// 		delete(store, args.Key)
//
// 		fmt.Printf("\nClient %v: del(%v)\n", args.ClientName, args.Key)
// 		t.mu.Unlock()
// 		return nil
// 	}
// 	fmt.Printf("\nClient %v: del(%v): key not found\n", args.ClientName, args.Key)
//
// 	errorMessage := fmt.Sprintf("del(%v): key not found", args.Key)
//
// 	t.mu.Unlock()
// 	return errors.New(errorMessage)
// }

func main() {
	// Functions aviable
	transaction := new(TransactionService)
	rpc.Register(transaction) // Register RPC service

	// Create store with x and y with value of 0
	store["x"] = 0
	store["y"] = 0

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
