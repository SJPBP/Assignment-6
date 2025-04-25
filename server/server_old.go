// server.go
// Prabhdeep Singh
// ps1282
package main

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"log"
	"sync"
)

var store = make(map[string]string)

// Output the whole store
func printStore() {
	fmt.Println("\nCurrent Key-Value Store:")

	for key, value := range store {
  	fmt.Printf("%v: %v\n", key, value)
  }
}

type PutArgs struct {
	ClientName, Key, Value string
}

type PutReply struct {
	// No Reply to send
}

type GetArgs struct {
	ClientName, Key string
}

type GetReply struct {
	Value string
}

type DelArgs struct {
	ClientName, Key string
}

type DelReply struct {
	// No Reply to send	
}

type Backend struct {
	int
	mu 	sync.Mutex
	rwMu sync.RWMutex
}

func (t *Backend) Put(args *PutArgs, reply *PutReply) error {
	t.mu.Lock()
	store[args.Key] = args.Value

	_, ok := store[args.Key]

	if ok == true {
		fmt.Printf("\nClient %v: put(%v,%v)\n", args.ClientName, args.Key, args.Value)
		printStore()

		t.mu.Unlock()
		return nil
	}
	t.mu.Unlock()

	return errors.New("Please provide key and value")
}

func (t *Backend) Get(args *GetArgs, reply *GetReply) error {
	t.rwMu.RLock()
	_, ok := store[args.Key]
	
	if ok == true {
		reply.Value = store[args.Key]

		fmt.Printf("\nClient %v: get(%v) = %v\n", args.ClientName, args.Key, reply.Value)

		t.rwMu.RUnlock()
		return nil
	}
	fmt.Printf("\nClient %v: get(%v): key not found\n", args.ClientName, args.Key)
	
	errorMessage := fmt.Sprintf("get(%v): key not found", args.Key)

	t.rwMu.RUnlock()
	return errors.New(errorMessage)
}

func (t *Backend) Del(args *DelArgs, reply *DelReply) error {
	t.mu.Lock()
	_, ok := store[args.Key]
	
	if ok == true {
		delete(store, args.Key)

		fmt.Printf("\nClient %v: del(%v)\n", args.ClientName, args.Key)
		t.mu.Unlock()
		return nil
	} 
	fmt.Printf("\nClient %v: del(%v): key not found\n", args.ClientName, args.Key)
	
	errorMessage := fmt.Sprintf("del(%v): key not found", args.Key)

	t.mu.Unlock()
	return errors.New(errorMessage)
}

func main()  {
	// Functions aviable
	arith := new(Backend)
	rpc.Register(arith) 												// Register RPC service


	listener, err := net.Listen("tcp", ":4000") // Create TCP listener on port 4000
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	defer listener.Close() 											// Close listener when main exits

	fmt.Println("Server listening on TCP port 1234")

	for { 																			// Note: this is an infinite loop
		conn, err := listener.Accept() 						// Accept the next incoming call and
		if err != nil { 													// return a connection
			fmt.Println("\nConnection error:", err)
			continue
		}
		go rpc.ServeConn(conn) 										// Serve RPC over the connection using a goroutine
	}
}
