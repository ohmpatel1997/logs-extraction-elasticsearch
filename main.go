package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {

	// ELASTIC SEARCH APPROACH

	// perform below operations first if we want to go for elastic search

	/* elasticsearch.CreateIndex()
	elasticsearch.InitializeReadTemplate()
	elasticsearch.RolloverIndexAPI()
	elasticsearch.AddInjestPipeline()
	ParseFile() */

	//then you can query logs from elasticsearch using QueryLogs() function
	args := os.Args[1:]
	if len(args) != 6 { // for format  LogExtractor.exe -f "From Time" -t "To Time" -i "Log file directory location"
		fmt.Println("Please give proper command line arguments")
		return
	}
	startTimeArg := args[1]
	finishTimeArg := args[3]
	fileName := args[5]

	queryStartTime, err := time.Parse("2006-01-02T15:04:05.0000Z", startTimeArg)
	if err != nil {
		fmt.Println("Could not able to parse the start time", startTimeArg)
		return
	}

	queryFinishTime, err := time.Parse("2006-01-02T15:04:05.0000Z", finishTimeArg)
	if err != nil {
		fmt.Println("Could not able to parse the start time", startTimeArg)
		return
	}

	file, err := os.Open(fileName)

	if err != nil {
		fmt.Println("Could not open the file", err)
		return
	}
	filestat, err := file.Stat()
	if err != nil {
		fmt.Println("Could not able to get the file stat")
		return
	}

	fileSize := filestat.Size()
	offset := fileSize - 1
	lastLineSize := 0

	for {
		b := make([]byte, 1)
		n, err := file.ReadAt(b, offset)
		if err != nil {
			fmt.Println("Error reading file ", err)
			break
		}
		char := string(b[0])
		if char == "\n" {
			break
		}
		offset--
		lastLineSize += n
	}

	lastLine := make([]byte, lastLineSize)
	_, err = file.ReadAt(lastLine, offset+1)

	if err != nil {
		fmt.Println("Could not able to read last line with offset", offset, "and lastline size", lastLineSize)
		return
	}

	logSlice := strings.SplitN(string(lastLine), ",", 2)
	logCreationTimeString := logSlice[0]

	lastLogCreationTime, err := time.Parse("2006-01-02T15:04:05.0000Z", logCreationTimeString)
	if err != nil {
		fmt.Println("can not able to parse time : ", err)
	}

	if lastLogCreationTime.After(queryStartTime) && lastLogCreationTime.Before(queryFinishTime) {
		ExtractLogs(file, queryStartTime, queryFinishTime)
	}
}

func ExtractLogs(file *os.File, start time.Time, end time.Time) {

	parsingStartAt := time.Now()
	scanner := bufio.NewScanner(file)

	linesChunkLen := 100 * 1024 //chunks of line to process

	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]string, 0, linesChunkLen)
		return lines
	}}
	lines := linesPool.Get().([]string)[:0]

	wg := sync.WaitGroup{}
	scanner.Scan()

	for { //we need to scan every line, as we dont know the size of each line in prior, so we cannt use file.Seek or file.readAt
		lines = append(lines, scanner.Text()) //this is a costly operation, we could have used channels if we knew the total lines in file
		willScan := scanner.Scan()
		if len(lines) == linesChunkLen || !willScan {
			linesToProcess := lines
			wg.Add(1) // add the count once every chunk of lines
			go func() {

				defer wg.Done()
				defer linesPool.Put(linesToProcess) // put back the line slice in pool

				for _, text := range linesToProcess {

					logSlice := strings.SplitN(text, ",", 2)
					logCreationTimeString := logSlice[0]

					logCreationTime, err := time.Parse("2006-01-02T15:04:05.0000Z", logCreationTimeString)
					if err != nil {
						fmt.Printf("\n Could not able to parse the time :%s for log : %v", logCreationTime, text)
						return
					}
					if logCreationTime.After(start) && logCreationTime.Before(end) {
						fmt.Println(text)
					}
				}

			}()
			lines = linesPool.Get().([]string)[:0] // get the new lines pool to store the new lines
		}
		if !willScan {
			break
		}
	}
	wg.Wait()
	fmt.Printf("\n time: %v\n", time.Since(parsingStartAt)) //processing will take much less time
	//fmt.Println(linesExtracted)                             //printing to console will take time
}
