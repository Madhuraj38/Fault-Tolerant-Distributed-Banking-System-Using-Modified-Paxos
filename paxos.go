package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Paxos struct {
	mu sync.Mutex
	//	rw              sync.RWMutex
	peers          []string
	me             int
	Balance        int
	db             *sql.DB
	dead           bool
	l              net.Listener
	ballot         string
	livePeers      map[int]bool
	highestBallot  string
	acceptNum      string
	acceptVal      []Transaction
	queue          []Transaction
	committedIndex int
}

type PrepareArgs struct {
	Ballot         string
	CommittedIndex int
}

type PrepareResponse struct {
	Promise        bool
	AcceptNum      string
	AcceptVal      []Transaction
	HighestBallot  string
	LocalTxs       []Transaction
	NeedSync       bool
	CommittedIndex int
}

type AcceptArgs struct {
	Ballot    string
	MegaBlock []Transaction
}

type AcceptResponse struct {
	Accepted bool
}

type DecideArgs struct {
	Ballot    string
	MegaBlock []Transaction
}

type DecideResponse struct {
	Success bool
}

type SyncArgs struct {
	MegaBlock []Transaction
}

func (px *Paxos) ProcessTransaction(tx Transaction) {
	maxRetryTime := 2 * time.Second
	startTime := time.Now()
	for !px.dead {
		px.mu.Lock()
		// if len(px.queue) > 0 {
		// 	for _, tx := range px.queue {
		// 		log.Printf("Transaction removed from server %d queue: %v and initiated paxos", px.me+1, tx)
		// 		go px.Start(tx)
		// 	}
		// 	px.queue = nil
		// }
		if px.Balance >= tx.Amount {
			px.Balance -= tx.Amount
			var lastID int
			err := px.db.QueryRow("SELECT IFNULL(max(seqId), 0) FROM local_log").Scan(&lastID)
			if err != nil {
				logger.Printf("failed to fetch max ID: %v", err)
			}

			lastID = lastID + 1
			_, err = px.db.Exec("INSERT INTO local_log (seqId, sender, receiver, amount) VALUES (?, ?, ?, ?)",
				lastID, tx.Sender, tx.Receiver, tx.Amount)
			px.mu.Unlock()
			if err != nil {
				logger.Printf("Server %d failed to insert transaction locally: %v", px.me+1, err)
			} else {
				logger.Printf("Server %d processed transaction locally: %v", px.me+1, tx)
			}
			return
		} else {
			px.mu.Unlock()
			logger.Printf("Server %d initiating Paxos for insufficient balance: %v", px.me+1, tx)
			if !px.Start(tx) {
				px.randomSleep()
			}

			if time.Since(startTime) > maxRetryTime {
				logger.Printf("Server %d timeout reached for transaction %v. Moving to next set.", px.me+1, tx)
				// px.mu.Lock()
				// px.queue = append(px.queue, tx)
				// logger.Printf("Transaction appended to server %d queue: %v", px.me+1, tx)
				// px.mu.Unlock()
				go func() {
					success := false
					for !success {
						success = px.Start(tx)
						if !success {
							px.randomSleep()
						}
					}
				}()
				return
			}
		}
	}
}

func (px *Paxos) Start(tx Transaction) bool {
	px.mu.Lock()
	px.ballot = fmt.Sprintf("%d#%d", time.Now().UnixNano(), px.me+1)
	px.mu.Unlock()

	promiseCount := 0
	var highestAcceptVal []Transaction
	localTxsArray := make([]Transaction, 0)
	responseChan := make(chan PrepareResponse, len(px.peers))
	timeout := time.After(150 * time.Millisecond)
	px.mu.Lock()
	args := PrepareArgs{Ballot: px.ballot, CommittedIndex: px.committedIndex}
	majority := len(px.peers)/2 + 1
	px.mu.Unlock()

	for i := range len(px.peers) {
		go func(peer int) {
			var reply PrepareResponse
			logger.Printf("Prepare call requested with ballot num %s from server %d to server %d", px.ballot, px.me+1, peer+1)

			if px.me == peer {
				px.Prepare(&args, &reply)
			} else {
				ok, err := px.call(px.peers[peer], "Paxos.Prepare", args, &reply)
				if !ok {
					logger.Printf("Prepare call failed for peer %d: %v", peer+1, err)
				}
			}

			responseChan <- reply
			// promise, peerAcceptNum, peerAcceptVal, peerLocalTxs := reply.Promise, reply.AcceptNum, reply.AcceptVal, reply.LocalTxs
			if px.me != peer && reply.NeedSync {
				go px.Synchronize(reply.CommittedIndex, peer)
			}
		}(i)
	}

	for {
		select {
		case reply := <-responseChan:
			px.mu.Lock()
			if reply.Promise {
				promiseCount++
				if reply.AcceptNum != "" && reply.AcceptVal != nil {
					if reply.AcceptNum > px.acceptNum {
						px.acceptNum = reply.AcceptNum
						highestAcceptVal = reply.AcceptVal
					}
				} else {
					localTxsArray = append(localTxsArray, reply.LocalTxs...)
				}
			}
			px.mu.Unlock()

		case <-timeout:
			logger.Println("Prepare phase timed out, proceeding with available promises")
			close(responseChan)
			if promiseCount >= majority {
				logger.Printf("Majority promises received by server %d, proceeding as leader.", px.me+1)
				megablock := px.createMegaBlock(highestAcceptVal, localTxsArray)
				if len(megablock) > 0 {
					return px.sendAccept(megablock)
				} else {
					logger.Printf("server %d has insufficent balance, aborting...", px.me+1)
					return false
				}
			} else {
				logger.Println("Failed to receive majority promises, aborting...")
				return false
			}
		}
	}

	// time.Sleep(100 * time.Millisecond)

}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareResponse) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if args.Ballot > px.highestBallot {
		if args.CommittedIndex == px.committedIndex {
			logger.Printf("Prepare call accepted with ballot num %s by server %d and prev highestballot : %s", args.Ballot, px.me+1, px.highestBallot)
			px.highestBallot = args.Ballot
			reply.Promise = true
			reply.HighestBallot = px.highestBallot
			if px.acceptNum != "" && px.acceptVal != nil {
				reply.AcceptNum = px.acceptNum
				reply.AcceptVal = px.acceptVal
				reply.LocalTxs = nil
			} else {
				reply.AcceptNum = ""
				reply.AcceptVal = nil
				reply.LocalTxs = px.getLocallyProcessedTransactions()
			}
		} else if args.CommittedIndex > px.committedIndex {
			reply.Promise = false
			reply.NeedSync = true
			px.acceptNum = ""
			px.acceptVal = nil
			reply.CommittedIndex = px.committedIndex
		} else {
			parts := strings.Split(args.Ballot, "#")
			if len(parts) == 2 {
				serverID, _ := strconv.Atoi(parts[1])
				if px.me != serverID-1 {
					go px.Synchronize(args.CommittedIndex, serverID-1)
				}
			} else {
				logger.Println("Invalid ballot number")
			}
			reply.Promise = false
		}
	} else {
		logger.Printf("Prepare call rejected with ballot num %s by server %d and prev highestballot : %s", args.Ballot, px.me+1, px.highestBallot)
		reply.Promise = false
		reply.HighestBallot = px.highestBallot
	}
	return nil
}

func (px *Paxos) createMegaBlock(peerTransactions []Transaction, localTxns []Transaction) []Transaction {
	var megablock []Transaction
	if len(peerTransactions) > 0 {
		megablock = peerTransactions
	} else {
		for _, tx := range localTxns {
			tx.CommitIndex = px.committedIndex
			megablock = append(megablock, tx)
		}
	}
	logger.Printf("Transactions : %v", megablock)
	return megablock
}

func (px *Paxos) sendAccept(megablock []Transaction) bool {
	px.mu.Lock()
	args := AcceptArgs{Ballot: px.ballot, MegaBlock: megablock}
	px.mu.Unlock()
	acceptCount := 0
	acceptChan := make(chan bool, len(px.peers))
	majority := len(px.peers)/2 + 1

	for i := range len(px.peers) {
		go func(peer int) {
			var reply AcceptResponse
			logger.Printf("Accept call requested with ballot num %s from server %d to server %d", px.ballot, px.me+1, peer+1)

			if px.me == peer {
				px.Accept(&args, &reply)
			} else {
				ok, err := px.call(px.peers[peer], "Paxos.Accept", args, &reply)
				if !ok {
					logger.Printf("Accept call failed for peer %d: %v", peer+1, err)
				}
			}

			acceptChan <- reply.Accepted
		}(i)
	}

	for i := 0; i < len(px.peers); i++ {
		accepted := <-acceptChan
		if accepted {
			acceptCount++
		}

		if acceptCount >= majority {
			logger.Printf("Majority accepts received: %d", acceptCount)
			px.sendDecide(megablock)
			return true
		}
	}

	logger.Printf("Failed to receive majority accepts, total accepts: %d", acceptCount)
	close(acceptChan)
	return false

}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptResponse) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if args.Ballot >= px.highestBallot {
		px.highestBallot = args.Ballot
		px.acceptVal = args.MegaBlock
		px.acceptNum = args.Ballot
		reply.Accepted = true
		logger.Printf("Accept call accepted with ballot num %s by server %d and prev highestballot : %s", args.Ballot, px.me+1, px.highestBallot)
	} else {
		reply.Accepted = false
	}
	return nil
}

func (px *Paxos) sendDecide(megablock []Transaction) {
	px.mu.Lock()
	args := DecideArgs{Ballot: px.ballot, MegaBlock: megablock}
	px.mu.Unlock()
	var leaderReply DecideResponse
	logger.Printf("Decide call requested with ballot num %s from server %d to server %d", px.ballot, px.me+1, px.me+1)
	px.ApplyDecision(&args, &leaderReply)
	if leaderReply.Success {
		logger.Printf("Committed successfully with ballot num %s by server %d", px.ballot, px.me+1)
		var wg sync.WaitGroup
		for i := range len(px.peers) {
			wg.Add(1)
			go func(peer int) {
				defer wg.Done()
				var reply DecideResponse
				logger.Printf("Decide call requested with ballot num %s from server %d to server %d", px.ballot, px.me+1, peer+1)
				if peer != px.me {
					ok, err := px.call(px.peers[peer], "Paxos.ApplyDecision", args, &reply)

					if !ok {
						logger.Printf("Decide request failed for peer %d: %v", peer, err)
					}
				} else {
					return
				}

				if reply.Success {
					logger.Printf("Committed successfully with ballot num %s by server %d", px.ballot, peer+1)
				} else {
					logger.Printf("Decide call rejected with ballot num %s by server %d", px.ballot, peer+1)
				}
			}(i)
		}
		wg.Wait()
	} else {
		logger.Printf("Decide call rejected with ballot num %s by server %d", px.ballot, px.me+1)
	}
}

func (px *Paxos) ApplyDecision(args *DecideArgs, reply *DecideResponse) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	var maxID int

	err := px.db.QueryRow("SELECT IFNULL(max(id), 0) FROM committed_log").Scan(&maxID)
	if err != nil {
		return fmt.Errorf("failed to fetch max ID: %w", err)
	}

	newID := maxID + 1
	if newID == px.committedIndex+1 {
		for _, tx := range args.MegaBlock {
			if num, _ := strconv.Atoi(tx.Receiver[1:]); num == px.me+1 {
				px.Balance += tx.Amount
			}

			_, err := px.db.Exec("INSERT INTO committed_log (id, sender, receiver, amount) VALUES (?, ?, ?, ?)",
				newID, tx.Sender, tx.Receiver, tx.Amount)
			if err != nil {
				logger.Printf("Error committing transaction on server %d: %v", px.me+1, err)
				reply.Success = false
				return err
			}

			if num, _ := strconv.Atoi(tx.Sender[1:]); num == px.me+1 {
				px.removeTransactionFromLocalLog(tx)
			}
		}
		px.committedIndex = newID
		px.acceptNum = ""
		px.acceptVal = nil
		reply.Success = true
	} else {
		reply.Success = false
	}
	return nil
}

func (px *Paxos) SynchronizeCommit(args *SyncArgs, reply *DecideResponse) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	for _, tx := range args.MegaBlock {
		if num, _ := strconv.Atoi(tx.Receiver[1:]); num == px.me+1 {
			px.Balance += tx.Amount
		}

		_, err := px.db.Exec("INSERT INTO committed_log (id, sender, receiver, amount) VALUES (?, ?, ?, ?)",
			tx.CommitIndex, tx.Sender, tx.Receiver, tx.Amount)
		if err != nil {
			logger.Printf("Error committing sync transaction on server %d: %v", px.me+1, err)
			reply.Success = false
			return err
		} else {
			px.committedIndex = tx.CommitIndex
		}
	}
	reply.Success = true
	return nil
}
func (px *Paxos) Synchronize(commitIndex int, peer int) bool {
	count := px.committedIndex - commitIndex
	for idx := commitIndex + 1; idx <= px.committedIndex; idx++ {
		rows, err := px.db.Query("SELECT id, sender, receiver, amount FROM committed_log WHERE id = ?", idx)
		if err != nil {
			logger.Printf("Error fetching committed transactions: %v", err)
			return false
		}
		defer rows.Close()

		var missedTransactions []Transaction
		for rows.Next() {
			var tx Transaction
			err := rows.Scan(&tx.CommitIndex, &tx.Sender, &tx.Receiver, &tx.Amount)
			if err != nil {
				logger.Printf("Error scanning committed transaction: %v", err)
				return false
			}
			missedTransactions = append(missedTransactions, tx)
		}
		args := SyncArgs{MegaBlock: missedTransactions}
		var reply DecideResponse
		logger.Printf("Sync requested from server %d to server %d", px.me+1, peer+1)
		ok, err := px.call(px.peers[peer], "Paxos.SynchronizeCommit", &args, &reply)

		if !ok {
			logger.Printf("Sync failed for peer %d: %v", peer, err)
			return false
		}
		if reply.Success {
			count = count - 1
		}
	}
	if count == 0 {
		logger.Printf("Successfully Sync from server %d to server %d", px.me+1, peer+1)
		return true
	}
	logger.Printf("Sync failed from server %d to server %d", px.me+1, peer+1)
	return false
}

func (px *Paxos) removeTransactionFromLocalLog(tx Transaction) {
	_, err := px.db.Exec("DELETE FROM local_log WHERE seqId = ? AND sender = ? AND receiver = ? AND amount = ?", tx.SeqId, tx.Sender, tx.Receiver, tx.Amount)
	if err != nil {
		logger.Printf("Error removing transaction from local log: %v", err)
	}
}

func (px *Paxos) getLocallyProcessedTransactions() []Transaction {
	var transactions []Transaction

	rows, err := px.db.Query("SELECT seqId, sender, receiver, amount FROM local_log")
	if err != nil {
		logger.Printf("Error retrieving local transactions: %v", err)
		return transactions
	}
	defer rows.Close()

	for rows.Next() {
		var tx Transaction
		err := rows.Scan(&tx.SeqId, &tx.Sender, &tx.Receiver, &tx.Amount)
		if err != nil {
			logger.Printf("Error scanning local log row: %v", err)
			continue
		}
		transactions = append(transactions, tx)
	}

	if err = rows.Err(); err != nil {
		logger.Printf("Error iterating over local log rows: %v", err)
	}

	return transactions
}

func (px *Paxos) call(srv string, name string, args interface{}, reply interface{}) (bool, error) {
	c, err := rpc.Dial("tcp", srv)
	if err != nil {
		logger.Printf("Failed to connect to peer %s : %v", srv, err)
		return false, err
	}
	defer c.Close()
	err = c.Call(name, args, reply)
	if err == nil {
		return true, err
	}

	logger.Println(err)
	return false, err
}

func (px *Paxos) randomSleep() {
	randomDuration := time.Duration(rand.Intn(51)+200) * time.Millisecond

	time.Sleep(randomDuration)
}

func Make(peers []string, me int) *Paxos {
	px := &Paxos{}
	px.mu = sync.Mutex{}
	//px.rw = sync.RWMutex{}
	px.peers = peers
	px.me = me
	px.Balance = 100
	px.livePeers = make(map[int]bool)
	px.ballot = ""
	px.acceptVal = nil
	px.acceptNum = ""
	px.queue = nil
	px.committedIndex = 0

	db, err := sql.Open("sqlite3", fmt.Sprintf("paxos_%d.db", me))
	if err != nil {
		logger.Fatalf("Error opening database: %v", err)
	}
	px.db = db

	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS local_log (
            seqId INTEGER,
            sender TEXT,
            receiver TEXT,
            amount INTEGER
        );
        CREATE TABLE IF NOT EXISTS committed_log (
            id INTEGER,
            sender TEXT,
            receiver TEXT,
            amount INTEGER
        );
    `)
	if err != nil {
		logger.Fatalf("Error creating tables: %v", err)
	}

	rpcs := rpc.NewServer()
	rpcs.Register(px)

	l, e := net.Listen("tcp", peers[me])
	if e != nil {
		logger.Fatal("listen error:", e)
	}
	px.l = l

	go func() {
		for {
			if !px.dead {
				conn, err := px.l.Accept()
				if err == nil && !px.dead {
					go rpcs.ServeConn(conn)
				} else if err != nil && !px.dead {
					logger.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}
	}()

	return px
}

func (px *Paxos) Kill() {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
	// px.db.Close()
	logger.Printf("Server %d set to inactive", px.me+1)
}

func (px *Paxos) Restart(me int) {

	if px.dead {
		px.dead = false
		var err error
		px.l, err = net.Listen("tcp", px.peers[me])
		if err != nil {
			logger.Fatalf("Failed to reopen listener for server %d: %v", px.me+1, err)
		}

		logger.Printf("Server %d successfully restarted", px.me+1)
	} else {
		logger.Printf("Server %d is already running, no need to restart", px.me+1)
	}
}

func (px *Paxos) PrintBalance() {
	px.mu.Lock()
	defer px.mu.Unlock()
	fmt.Printf("Server %d balance: %d\n", px.me+1, px.Balance)
}

func (px *Paxos) PrintLog() {
	px.mu.Lock()
	defer px.mu.Unlock()

	fmt.Printf("Server %d local log:\n", px.me+1)
	rows, err := px.db.Query("SELECT seqId, sender, receiver, amount FROM local_log")
	if err != nil {
		log.Printf("Error querying local log: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var sender, receiver string
		var amount, seqId int
		err := rows.Scan(&seqId, &sender, &receiver, &amount)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}
		fmt.Printf(" %d. %s -> %s: %d\n", seqId, sender, receiver, amount)
	}
}

func (px *Paxos) PrintDB() {
	px.mu.Lock()
	defer px.mu.Unlock()

	fmt.Printf("Server %d committed log:\n", px.me+1)
	rows, err := px.db.Query("SELECT id, sender, receiver, amount FROM committed_log")
	if err != nil {
		log.Printf("Error querying committed log: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var sender, receiver string
		var amount, id int
		err := rows.Scan(&id, &sender, &receiver, &amount)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}
		fmt.Printf(" %d. %s -> %s: %d\n", id, sender, receiver, amount)
	}
}

func cleanupDBFiles() {
	files, err := os.ReadDir(".")
	if err != nil {
		logger.Printf("Error reading directory: %v", err)
		return
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".db") {
			err := os.Remove(file.Name())
			if err != nil {
				logger.Printf("Error deleting file %s: %v", file.Name(), err)
			} else {
				logger.Printf("Deleted file: %s", file.Name())
			}
		}
	}
}
