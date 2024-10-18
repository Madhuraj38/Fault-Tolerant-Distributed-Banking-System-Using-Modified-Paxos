package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Transaction struct {
	SeqId       int
	Sender      string
	Receiver    string
	Amount      int
	CommitIndex int
}

type TransactionSet struct {
	SetNumber    int
	Transactions []Transaction
	LiveServers  []int
}

var logger *log.Logger
var (
	clientBalances = map[int]int{
		1: 100,
		2: 100,
		3: 100,
		4: 100,
		5: 100,
	}
)

func initLogger() {
	file, err := os.OpenFile("transactions.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	logger = log.New(file, "LOG: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func ParseTransactionsFromCSV(filename string) ([]TransactionSet, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var transactionSets []TransactionSet
	var currentSet TransactionSet

	for i, record := range records {
		if i == 0 {
			record[0] = strings.TrimPrefix(record[0], "\ufeff")
		}
		if record[0] != "" {
			setNumber, _ := strconv.Atoi(record[0])
			if setNumber != currentSet.SetNumber {
				if currentSet.SetNumber != 0 {
					transactionSets = append(transactionSets, currentSet)
				}
				currentSet = TransactionSet{SetNumber: setNumber, LiveServers: parseLiveServers(record[2])}
			}
		}

		tx := parseTransaction(record[1])
		currentSet.Transactions = append(currentSet.Transactions, tx)

	}

	if currentSet.SetNumber != 0 {
		transactionSets = append(transactionSets, currentSet)
	}

	return transactionSets, nil
}

func parseTransaction(input string) Transaction {
	input = strings.Trim(input, "()")
	parts := strings.Split(input, ",")
	amount, _ := strconv.Atoi(strings.TrimSpace(parts[2]))
	return Transaction{
		Sender:   strings.TrimSpace(parts[0]),
		Receiver: strings.TrimSpace(parts[1]),
		Amount:   amount,
	}
}

func parseLiveServers(input string) []int {
	input = strings.Trim(input, "[]")
	serverStrings := strings.Split(input, ",")
	var servers []int

	for _, server := range serverStrings {
		server = strings.TrimSpace(server)
		if len(server) > 1 && server[0] == 'S' {
			if num, err := strconv.Atoi(server[1:]); err == nil {
				servers = append(servers, num-1)
			}
		}
	}

	return servers
}

func processTransactionSet(set TransactionSet, pxa []*Paxos) (map[int][]Transaction, map[int]int64) {
	txBySender := make(map[int][]Transaction)
	elapsedTimes := make(map[int]int64)
	// for i := 0; i < peerSize; i++ {
	// 	pxa[i].mu.Lock()
	// 	if len(pxa[i].queue) > 0 {
	// 		log.Printf("Transactions removed from server %d queue and added to client", i+1)
	// 		txBySender[i] = append(txBySender[i], pxa[i].queue...)
	// 		pxa[i].queue = nil
	// 	}
	// 	pxa[i].mu.Unlock()
	// }

	for _, tx := range set.Transactions {
		senderIndex := int(tx.Sender[1] - '1')
		txBySender[senderIndex] = append(txBySender[senderIndex], tx)
	}
	fmt.Println("Processing...")
	var wg sync.WaitGroup
	for senderIndex, transactions := range txBySender {
		wg.Add(1)
		go func(index int, txs []Transaction) {
			defer wg.Done()
			startTime := time.Now()
			for _, tx := range txs {
				pxa[index].ProcessTransaction(tx)
			}
			endTime := time.Now()
			elapsedTimeMs := endTime.Sub(startTime).Milliseconds()
			elapsedTimes[index] = elapsedTimeMs
		}(senderIndex, transactions)

	}
	wg.Wait()
	return txBySender, elapsedTimes
}

func getBalances(set TransactionSet) {

	for _, tx := range set.Transactions {
		sender, _ := strconv.Atoi(tx.Sender[1:])
		receiver, _ := strconv.Atoi(tx.Receiver[1:])
		clientBalances[sender] -= tx.Amount
		clientBalances[receiver] += tx.Amount
	}
	fmt.Println("Client Balances :")
	for client, balance := range clientBalances {
		fmt.Printf("Client %d balance : %d\n", client, balance)
	}
	fmt.Println()
}

func Performance(txBySender map[int][]Transaction, elapsedTimeMs map[int]int64) {
	var totalLatency int64
	var totalThroughput float64

	for client, elapsedTime := range elapsedTimeMs {
		clientTransactions := len(txBySender[client])

		if clientTransactions > 0 {
			totalLatency += elapsedTime
			totalThroughput += float64(clientTransactions) / (float64(elapsedTime) / 1000.0)
		}
	}

	fmt.Printf("Latency: %d ms\n", totalLatency)
	fmt.Printf("Throughput (transactions per second): %.2f\n\n", totalThroughput)
}

func main() {
	peers := []string{"localhost:1111", "localhost:2222", "localhost:3333", "localhost:4444", "localhost:5555"}
	pxa := make([]*Paxos, len(peers))
	liveservers := make(map[int]bool)

	initLogger()

	transactionSets, err := ParseTransactionsFromCSV("transactions.csv")
	if err != nil {
		log.Fatalf("Error parsing transactions: %v", err)
	}

	cleanupDBFiles()

	for i := 0; i < len(peers); i++ {
		pxa[i] = Make(peers, i)
		liveservers[i] = false
		//defer pxa[i].Kill()
	}

	reader := bufio.NewReader(os.Stdin)
	for _, set := range transactionSets {
		fmt.Printf("Press Enter to process Set %d...\n", set.SetNumber)
		reader.ReadString('\n')
		// fmt.Println(set.LiveServers)
		// for _, px := range pxa {
		// 	px.UpdateLiveServers(set.LiveServers)
		// }
		logger.Printf("Started processing transactions of set %d : ", set.SetNumber)
		logger.Println("-------------------------------------------------------------------------------------------")
		for i := 0; i < len(peers); i++ {
			liveservers[i] = false
		}

		for _, server := range set.LiveServers {
			liveservers[server] = true
		}
		for i := 0; i < len(peers); i++ {
			if !liveservers[i] {
				logger.Printf("liveserver %d is %t so killing", i+1, liveservers[i])
				pxa[i].Kill()
			} else {
				pxa[i].mu.Lock()
				if pxa[i].dead {
					logger.Printf("liveserver %d is true but server %d is dead, restarting", i+1, i+1)
					pxa[i].Restart(i)
				}
				pxa[i].mu.Unlock()
			}
		}

		txBySender, elapsedTimeMs := processTransactionSet(set, pxa)

		logger.Println("---------------------------------------------------------------------------------------------")
		fmt.Println("\nAll transactions in the set have been processed.")
		fmt.Println("Press Enter to view results...")
		reader.ReadString('\n')

		for _, px := range pxa {
			px.PrintBalance()
			px.PrintLog()
			px.PrintDB()
			fmt.Println()
		}
		fmt.Printf("Set %d - Performance Metrics:\n", set.SetNumber)
		Performance(txBySender, elapsedTimeMs)
		getBalances(set)
	}
}
