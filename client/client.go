// client.go
// Usage: go run client.go <# clients>

package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"sync"
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

func transactionWorker(client *rpc.Client, wg *sync.WaitGroup, name string) {
	defer wg.Done()

	beginArgs := &BeginArgs{TxID: name}
	beginReply := &BeginReply{}
	err := client.Call("TransactionService.BeginTransaction", beginArgs, beginReply)
	if err != nil {
		log.Printf("[%s] Begin error: %v", name, err)
		return
	}
	ts := beginReply.Timestamp
	log.Printf("[%s] Began transaction with timestamp %d\n", name, ts)

	keys := []string{"x", "y"}

	// Perform random read/write operations
	for i := 0; i < 3; i++ {
		key := keys[rand.Intn(len(keys))]
		op := rand.Intn(2) // 0 = read, 1 = write

		if op == 0 {
			readArgs := &ReadArgs{TxID: name, Key: key}
			readReply := &ReadReply{}
			err = client.Call("TransactionService.Read", readArgs, readReply)
			if err != nil || readReply.Error != "" {
				log.Printf("[%s] Read error: %v %s", name, err, readReply.Error)
				return
			}
			log.Printf("[%s] Read %s = %d", name, key, readReply.Value)
		} else {
			value := rand.Intn(500)
			writeArgs := &WriteArgs{TxID: name, Key: key, Value: value}
			writeReply := &WriteReply{}
			err = client.Call("TransactionService.Write", writeArgs, writeReply)
			if err != nil || writeReply.Error != "" {
				log.Printf("[%s] Write error: %v %s", name, err, writeReply.Error)
				return
			}
			log.Printf("[%s] Wrote %s = %d", name, key, value)
		}

		time.Sleep(time.Millisecond * time.Duration(rand.Intn(200))) // small random delay
	}

	// Commit transaction
	commitArgs := &CommitArgs{TxID: name}
	commitReply := &CommitReply{}
	err = client.Call("TransactionService.Commit", commitArgs, commitReply)
	if err != nil || !commitReply.Success {
		log.Printf("[%s] Commit failed: %v %s", name, err, commitReply.Error)
		return
	}
	log.Printf("[%s] Commit succeeded", name)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go <# clients>")
		return
	}

	numClients, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid number:", os.Args[1])
		return
	} else if numClients > 7 {
		fmt.Println("Number of clients <= 7")
		return
	}

	rand.Seed(time.Now().UnixNano())
	client, err := rpc.Dial("unix", "/tmp/rpc.sock")
	// client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("Dialing error:", err)
	}

	var wg sync.WaitGroup

	names := []string{"Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace"}

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go transactionWorker(client, &wg, names[i])
	}

	wg.Wait()
}
