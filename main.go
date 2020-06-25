package main

import (
	"bufio"
	"fmt"
	//"github.com/olivere/elastic"
	"log"
	"os"
	"strings"
	"sync"

	"time"
)

type Log struct {
	CreatedOn time.Time `json:"created_on"`
	Message   string    `json:"message"`
}

func main() {
	start := time.Now()
	file, err := os.Open("logs.log")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	linesChunkLen := 10000 * 1024

	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]string, 0, linesChunkLen)
		return lines
	}}
	lines := linesPool.Get().([]string)[:0]

	logsPool := sync.Pool{New: func() interface{} {
		entries := make([]Log, 0, linesChunkLen)
		return entries
	}}

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

					if entry.CreatedOn, err = time.Parse("2006-01-02T15:04:05.0000Z", logCreationTime); err != nil {
						fmt.Printf("Could not able to parse the time :%s for log : %v", logCreationTime, text)
						return
					}
					entries = append(entries, entry)
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

func ParseAndIndexBulk()
