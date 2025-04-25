// client.go
// Prabhdeep Singh
// ps1282
package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"bufio"
	"net/rpc"
	"math/rand"
	"time"
)


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

func sendToPut(client *rpc.Client, args PutArgs, reply *PutReply)  {
	err := client.Call("Backend.Put", args, &reply)

	// fmt.Printf("put %v %v\n", args.Key, args.Value)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("put(%v,%v) successful\n\n", args.Key, args.Value)
}

func sendToGet(client *rpc.Client, args GetArgs, reply *GetReply)  {
	err := client.Call("Backend.Get", args, &reply)
	
	// fmt.Printf("get %v\n", args.Key)

	if err != nil {
		fmt.Printf("get(%v): key not found\n\n", args.Key)
	} else {
		fmt.Printf("get(%v) = %v\n\n", args.Key, reply.Value)
	}

}

func sendToDel(client *rpc.Client, args DelArgs, reply *DelReply)  {
	err := client.Call("Backend.Del", args, &reply)

	// fmt.Printf("del %v\n", args.Key)


	if err != nil {
		fmt.Printf("del(%v): key not found\n\n", args.Key)
	} else {
		fmt.Printf("del(%v) successful\n\n", args.Key)
	}

}

func Quit(client *rpc.Client) {
	fmt.Printf("Exiting...\n\n")
	
	client.Close()
}

func run(tokens []string, clientName string, client *rpc.Client)  {
	
	// Get the command
	command := string(tokens[0])

	// Empty global key and value variable
	// Because creating the variable causes created but not used error
	var key string
	var value string

	// Get different key and value based the command
	if command == "get" {
			key = tokens[1]
			// value = ""

			getArgs := GetArgs{clientName, key}
			getReply := new(GetReply)

			sendToGet(client, getArgs, getReply)

	} else if command == "put" {
		key = tokens[1]
		value = tokens[2]

		putArgs := PutArgs{clientName, key, value}
		putReply := new(PutReply)

		sendToPut(client, putArgs, putReply)

	} else if command == "del" {
		key = tokens[1]
		// value = ""

		delArgs := DelArgs{clientName, key}
		delReply := new(DelReply)

		sendToDel(client, delArgs, delReply)

	} else if command == "quit" {
		// key = ""
		// value = ""
		Quit(client)


	} else {
		fmt.Println("Unkown command found in the file.")
	}
}

// Gives random number
func randSec(minSec int, maxSec int) int {
	return minSec + rand.Intn(maxSec - minSec)
}

func sleep(minSec int, maxSec int) {
    duration := randSec(minSec, maxSec) // Get random time duration to wait
    time.Sleep(time.Duration(duration) * time.Millisecond) // Sleep for given duration
}

func main()  {
	//TODO: Takes Alice and file name as argument
	//TODO: Take command from the file
	//TODO: Wait Random millisecond before running the command
	
	clientName := os.Args[1]

	//TODO: Add getting ip address when changing from unix to tcp
	serverAddress := os.Args[2]
	filePath := os.Args[3]

	client, err := rpc.Dial("tcp", serverAddress+":1234") // Connect to RPC server via TCP
	if err != nil {
		log.Fatal("Dialing error:", err)
	}
	defer client.Close()
	

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	
	// Allows to generate random number each time rand is called
	rand.Seed(time.Now().UTC().UnixNano())   


	for scanner.Scan() {
		// Get a line from file 
		line := scanner.Text()

		// Separate the line by space
		tokens := strings.Fields(line)
	
		// Randomly wait between 2 to 2000 millisecond
		// Or 0.2 to 2 seconds
		sleep(2, 2000)

		// Run different functions based on input
		run(tokens, clientName, client)
		// command, key, value := extract(tokens) 
	}
}
