// server.go
// Prabhdeep Singh
// David Majomi
// dom22
// ps1282
package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"slices"
	"time"
)

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

type Timestamp int64

type Transaction struct {
	Timestamp Timestamp
	TxID      string
	Key       []string
}

// Create map to store every transaction
var transaction = make(map[string]*Transaction)

// Struct for key used for transaction
type KEY struct {
	committed_value     int
	committed_timestamp Timestamp
	RTS                 []Timestamp
	TW                  map[Timestamp]int
	waitChans           map[Timestamp]chan bool
	// waitChans           chan bool
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
	transaction[args.TxID] = &Transaction{
		Timestamp: ts,
		TxID:      args.TxID,
	}

	return nil
}

func get_D_of_maximum_TW(Tc Timestamp, key *KEY) Timestamp {
	max_TW := key.committed_timestamp // Start from committed timestamp
	var found bool

	for ts := range key.TW {
		if ts <= Tc {
			if !found || ts > max_TW {
				max_TW = ts
				found = true
			}
		}
	}
	return max_TW
}

func remove_Tc_from_TW_and_waitChans(Tc Timestamp, key *KEY) {
	// Remove Tc from tentative writes
	if _, exists := key.TW[Tc]; exists {
		delete(key.TW, Tc)
	}

	// Remove Tc from waitChans
	if ch, exists := key.waitChans[Tc]; exists {
		// First, close the channel
		close(ch)
		// Then delete the entry
		delete(key.waitChans, Tc)
	}
}

func add_RTS(Tc Timestamp, key *KEY) {
	// Add Tc to RTS if not already in it
	if !slices.Contains(key.RTS, Tc) {
		key.RTS = append(key.RTS, Tc)
	}
}

func (s TransactionService) Read(args *ReadArgs, reply *ReadReply) error {
	fmt.Println("\nRunning Read Method")

	// Get client transaction
	tx, ok := transaction[args.TxID]

	if ok {
		// Get Tc of client
		Tc := tx.Timestamp

		// Get data for key the client asked for
		key, ok := store[args.Key]

		if ok {
			for {
				if Tc > key.committed_timestamp {
					// version of D with the maximum write timestamp ≤ Tc
					max_TW := get_D_of_maximum_TW(Tc, key)

					if key.committed_timestamp == max_TW {
						// Return the value of D_selected
						reply.Value = key.TW[max_TW]

						// Update the RTS with Tc
						add_RTS(Tc, key)

						break

					} else if max_TW == Tc {
						// Return the value of D_selected
						reply.Value = key.TW[max_TW]

						// Update the RTS with Tc
						add_RTS(Tc, key)

						break

					} else {
						fmt.Println(max_TW)
						// Get the channel for tentative write
						if ch, exists := key.waitChans[max_TW]; exists {
							// Wait for the channel to be closed
							// It means either transaction was abortted or committed

							<-ch
						}

						continue
					}
				} else {

					reply.Error = "Aborted: read timestamp <= committed write timestamp"

					// Remove Tc from tentative write
					remove_Tc_from_TW_and_waitChans(Tc, key)

					errorMessage := fmt.Sprintf("Aborted: read timestamp <= committed write timestamp - Read %v", args.Key)
					return errors.New(errorMessage)
				}
			}
		} else {
			reply.Error = "Key not found"
		}
	} else {
		reply.Error = "Transaction not found"
	}
	return nil
}

func get_max_RTS(key *KEY) Timestamp {
	if len(key.RTS) == 0 {
		return Timestamp(0) // assume there is a value with timestamp of 0
	}

	max := key.RTS[0] // assume first element is max
	for _, rts := range key.RTS {
		if rts > max {
			max = rts
		}
	}

	return max
}

func (s *TransactionService) Write(args *WriteArgs, reply *WriteReply) error {
	fmt.Println("\nDoing Write Method")
	// Get client transaction
	tx, ok := transaction[args.TxID]

	if ok {
		// Get Tc of client
		Tc := tx.Timestamp

		// Get key the client asked for
		key, ok := store[args.Key]

		if ok {
			max_RTS := get_max_RTS(key)

			max_TW := key.committed_timestamp

			if Tc >= Timestamp(max_RTS) {
				if Tc > max_TW {
					// Add client timestamp and value to tentative write
					key.TW[Tc] = args.Value

					// Tell what key the transaction is working on
					tx.Key = append(tx.Key, args.Key)

					// Create a channel for Tc
					newChan := make(chan bool)

					// and add it to channels map
					key.waitChans[Tc] = newChan

				} else {
					reply.Error = "Aborted: write timestamp <= committed write timestamp"

					// Remove Tc from tentative write
					remove_Tc_from_TW_and_waitChans(Tc, key)

					errorMessage := fmt.Sprintf("Aborted: write timestamp <= committed write timestamp - Write %v = %v", args.Key, args.Value)
					return errors.New(errorMessage)

				}
			} else {
				reply.Error = "Aborted: write timestamp < max read timestamp"

				// Remove Tc from tentative write
				remove_Tc_from_TW_and_waitChans(Tc, key)

				errorMessage := fmt.Sprintf("Aborted: write timestamp < max read timestamp - Write %v = %v", args.Key, args.Value)
				return errors.New(errorMessage)

			}
		} else {
			reply.Error = "Key not found"
		}
	} else {
		reply.Error = "Transaction not found"
	}

	return nil
}

func (s *TransactionService) Commit(args *CommitArgs, reply *CommitReply) error {
	fmt.Printf("[%s] Attempting to commit\n", args.TxID)
	
	// Get transaction
	tx, ok := transaction[args.TxID]
	if !ok {
		reply.Error = "Transaction not found"
		fmt.Printf("[%s] Error: %s\n", args.TxID, reply.Error)
		return errors.New(reply.Error)
	}

	// For each key the transaction has written to
	for _, keyName := range tx.Key {
		key := store[keyName]

		// Wait for all transactions with lower timestamps in TW to complete
		for otherTs := range key.TW {
			if otherTs < tx.Timestamp {
				if ch, exists := key.waitChans[otherTs]; exists {
					fmt.Printf("[%s] Waiting for transaction with timestamp %v to finish...\n", args.TxID, otherTs)
					<-ch
				}
			}
		}

		// After waiting, apply the tentative write to the store
		if val, exists := key.TW[tx.Timestamp]; exists {
			key.committed_value = val
			key.committed_timestamp = tx.Timestamp
			// Remove from TW but keep in RTS
			delete(key.TW, tx.Timestamp)
		}


		// Print the key state
		fmt.Printf("\n%v state:\n", tx.Key)
		fmt.Printf("committed value = %v\n", key.committed_value)
		fmt.Printf("committed timestamp = %v\n", key.committed_timestamp)
		fmt.Printf("RTS: %v\n", key.RTS)
		fmt.Printf("TW: %v\n", key.TW)
		fmt.Printf("waitChan: %v\n", key.waitChans)

		// Close the wait channel to notify other transactions
		if ch, exists := key.waitChans[tx.Timestamp]; exists {
			close(ch)
			delete(key.waitChans, tx.Timestamp)
		}

		// Print state of the key after commit
		fmt.Printf("[%s] Commit value of %s = %d\n", args.TxID, keyName, key.committed_value)
	}

	// Print commit success message
	fmt.Printf("[%s] Commit succeeded\n", args.TxID)
	
	// Make sure to also print the other key's value if not in tx.Key
	allKeys := []string{"x", "y"}
	for _, keyName := range allKeys {
		keyFound := false
		for _, txKey := range tx.Key {
			if txKey == keyName {
				keyFound = true
				break
			}
		}
		if !keyFound {
			fmt.Printf("[%s] Commit value of %s = %d\n", args.TxID, keyName, store[keyName].committed_value)
		}
	}

	reply.Success = true
	return nil
}

func main() {
	// Create store with x and y with value of 0 to all
	// Create the abort channel

	fmt.Printf("Starting server...")
	store["x"] = &KEY{
		committed_value:     0,
		committed_timestamp: 0,
		RTS:                 make([]Timestamp, 0),
		TW:                  make(map[Timestamp]int),
		waitChans:           make(map[Timestamp]chan bool),
	}

	store["y"] = &KEY{
		committed_value:     0,
		committed_timestamp: 0,
		RTS:                 make([]Timestamp, 0),
		TW:                  make(map[Timestamp]int),
		waitChans:           make(map[Timestamp]chan bool),
	}

	// store["x"] = &KEY{} // Create empty KEY
	// store["x"].committed_value = 0
	// store["x"].committed_timestamp = 0
	// store["x"].RTS = make([]Timestamp, 0)
	// store["x"].TW = make(map[Timestamp]int)
	//
	// store["y"] = &KEY{} // Create empty KEY
	// store["y"].committed_value = 0
	// store["y"].committed_timestamp = 0
	// store["y"].RTS = make([]Timestamp, 0)
	// store["y"].TW = make(map[Timestamp]int)

	// store["y"].RTS = append(store["y"].RTS, Timestamp(0))

	// Create timestamp with 0 value
	// ts := Timestamp(0)

	// Create TW and put timestamp and value to 0
	// store["x"].TW[ts] = 0

	// Create TW and put timestamp and value to 0
	// store["y"].TW[ts] = 0

	// Functions aviable
	transaction := new(TransactionService)
	rpc.Register(transaction) // Register RPC service

	sockAddr := "/tmp/rpc.sock"
	os.Remove(sockAddr)

	listener, err := net.Listen("unix", sockAddr)
	// listener, err := net.Listen("tcp", ":4000") // Create TCP listener on port 4000
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	defer listener.Close() // Close listener when main exits

	// fmt.Println("Server listening on TCP port 1234")

	for { // Note: this is an infinite loop
		conn, err := listener.Accept() // Accept the next incoming call and
		if err != nil {                // return a connection
			fmt.Println("\nConnection error:", err)
			continue
		}
		go rpc.ServeConn(conn) // Serve RPC over the connection using a goroutine
	}
}
