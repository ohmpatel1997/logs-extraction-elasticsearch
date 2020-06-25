package main

import (
	"bufio"
	"fmt"
	//"github.com/olivere/elastic"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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

	linesChunkLen := 100000 * 1024
	linesChunkPoolAllocated := int64(0)
	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]string, 0, linesChunkLen)
		atomic.AddInt64(&linesChunkPoolAllocated, 1)
		return lines
	}}
	lines := linesPool.Get().([]string)[:0]

	logsPoolAllocated := int64(0)
	logsPool := sync.Pool{New: func() interface{} {
		entries := make([]Log, 0, linesChunkLen)
		atomic.AddInt64(&logsPoolAllocated, 1)
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

				entries := logsPool.Get().([]Log)[:0]
				for _, text := range linesToProcess {

					entry := Log{}
					timeStampEndingIndex := strings.Index(text, ",")
					entry.Message = text[:timeStampEndingIndex]
					logCreationTime := text[timeStampEndingIndex+1:]

					if entry.CreatedOn, err = time.Parse("2006-01-02T15:04:05-0700Z", logCreationTime); err != nil {
						fmt.Printf("Could not able to parse the time :%s for log : %v", logCreationTime, entry)
						return
					}
					entries = append(entries, entry)
				}
				linesPool.Put(linesToProcess) // put back the line slice in pool
				//ParseAndIndexBulk()
				logsPool.Put(entries) //put back the log slice in pool
				wg.Done()             //decrease the count
			}()
			lines = linesPool.Get().([]string)[:0] // get the new lines pool to store the new lines
		}
		if !willScan {
			break
		}
	}
	wg.Wait()
	fmt.Printf("Name time: %v\n", time.Since(start))

}

//func ParseAndIndexBulk()
