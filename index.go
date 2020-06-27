package main

import (
	"bufio"

	"context"
	"encoding/json"

	"fmt"

	"github.com/ohmpatel1997/logs-extraction-elasticsearch/common"
	elastic "github.com/olivere/elastic"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	FILENAME = "logs.log"
)

type Log struct {
	CreatedOn time.Time `json:"created_on"`
	Message   string    `json:"message"`
	Line      int       `json:"line_no"`
}

func ParseFile() {

	file, err := os.Open(FILENAME)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	esClient, err := common.GetClient()

	if err != nil {
		log.Fatal(err)
	}
	c := context.Background()
	ParseAndIndexFile(c, file, esClient)
}

func ParseAndIndexFile(c context.Context, file *os.File, client *elastic.Client) {

	start := time.Now()
	scanner := bufio.NewScanner(file)

	linesChunkLen := 500 * 1024 //chunks of line to process

	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]string, 0, linesChunkLen)
		return lines
	}}
	lines := linesPool.Get().([]string)[:0]

	logsPool := sync.Pool{New: func() interface{} {
		entries := make([]Log, 0, linesChunkLen)
		return entries
	}}
	lock := sync.Mutex{}
	count := 0
	wg := sync.WaitGroup{}
	scanner.Scan()
	for {
		lines = append(lines, scanner.Text())
		willScan := scanner.Scan()
		if len(lines) == linesChunkLen || !willScan {
			linesToProcess := lines
			wg.Add(1) // add the count once every 100000 lines
			go func() {
				//decrease the count
				defer wg.Done()
				entries := logsPool.Get().([]Log)[:0]
				defer linesPool.Put(linesToProcess) // put back the line slice in pool
				//ParseAndIndexBulk()
				defer logsPool.Put(entries) //put back the log slice in pool

				for _, text := range linesToProcess {

					entry := Log{}
					logSlice := strings.SplitN(text, ",", 2)
					logCreationTime := logSlice[0]
					entry.Message = logSlice[1]

					lock.Lock()
					count++
					entry.Line = count
					lock.Unlock()
					var err error
					if entry.CreatedOn, err = time.Parse("2006-01-02T15:04:05.0000Z", logCreationTime); err != nil {
						fmt.Printf("Could not able to parse the time :%s for log : %v", logCreationTime, text)
						return
					}
					entries = append(entries, entry)
				}

				_, err := ParseAndIndexBulk(c, client, entries)
				if err != nil {
					fmt.Printf("Could not able to index the entries :%s", err.Error())
				}
			}()
			lines = linesPool.Get().([]string)[:0] // get the new lines pool to store the new lines
		}
		if !willScan {
			break
		}
	}
	wg.Wait()
	fmt.Printf("\n time: %v\n", time.Since(start))
}

func ParseAndIndexBulk(c context.Context, client *elastic.Client, entries []Log) (res *elastic.BulkResponse, err error) {

	bulk := client.Bulk()
	for _, log := range entries {

		req := elastic.NewBulkIndexRequest()
		jsonData, err := json.Marshal(log)
		if err != nil {
			return nil, err
		}
		req = req.OpType("index")
		req = req.Index("daily_logs")
		req = req.Type("_doc")
		req = req.Doc(string(jsonData))
		bulk = bulk.Add(req)
	}

	bulk.Pipeline("dailyindex")
	bulk.Pretty(true)
	bulk.Human(true)

	bulkResp, err := bulk.Do(c)
	if err != nil {
		return nil, err
	}

	return bulkResp, nil
}
