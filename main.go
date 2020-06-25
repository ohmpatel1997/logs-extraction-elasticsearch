package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
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
			wg.Add(len(linesToProcess))
			go func() {
				entries := logsPool.Get().([]Log)[:0]
				for _, text := range linesToProcess {

					entry := Log{}
					timeStampEndingIndex := strings.Index(text, ",")
					entry.Message = text[:timeStampEndingIndex]
					time := text[timeStampEndingIndex+1:]
					time.parse
					entries = append(entries, entry)
				}
				linesPool.Put(linesToProcess)
				mutex.Lock()
				for _, entry := range entries {
					if len(entry.firstName) != 0 {
						nameCount := nameMap[entry.firstName] + 1
						nameMap[entry.firstName] = nameCount
						if nameCount > commonCount {
							commonCount = nameCount
							commonName = entry.firstName
						}
					}
					if namesCounted == false {
						if namesCount == 0 {
							fmt.Printf("Name: %s at index: %v\n", entry.name, 0)
						} else if namesCount == 432 {
							fmt.Printf("Name: %s at index: %v\n", entry.name, 432)
						} else if namesCount == 43243 {
							fmt.Printf("Name: %s at index: %v\n", entry.name, 43243)
							namesCounted = true
						}
						namesCount++
					}
					dateMap[entry.date]++
				}
				mutex.Unlock()
				entriesPool.Put(entries)
				wg.Add(-len(entries))
			}()
			lines = linesPool.Get().([]string)[:0]
		}
		if !willScan {
			break
		}
	}
	wg.Wait()

	// report c2: names at index
	fmt.Printf("Name time: %v\n", time.Since(start))

	// report c1: total number of lines
	fmt.Printf("Total file line count: %v\n", fileLineCount)
	fmt.Printf("Line count time: %v\n", time.Since(start))

	// report c3: donation frequency
	for k, v := range dateMap {
		fmt.Printf("Donations per month and year: %v and donation ncount: %v\n", k, v)
	}
	fmt.Printf("Donations time: %v\n", time.Since(start))

	// report c4: most common firstName
	fmt.Printf("The most common first name is: %s and it occurs: %v times.\n", commonName, commonCount)
	fmt.Printf("Most common name time: %v\n", time.Since(start))
}
