package main

import (
	"bytes"
	"fmt"
	"golang.org/x/exp/mmap"
	"math"
	"strconv"
	"strings"
	"time"
)

type data struct {
	min float64
	max float64

	numPoints int
	total     float64
}

func main() {
	start := time.Now()
	readerAt, err := mmap.Open("data/measurements.csv")
	if err != nil {
		panic(err)
	}
	results := map[string]*data{}
	cpus := 32
	out := make(chan map[string]*data)
	b := make([]byte, readerAt.Len())
	readerAt.ReadAt(b, 0)

	// cpus-1 because chunkBytes always returns one extra chunk due to usage of LastIndexByte creating leftovers
	chunks := chunkBytes(b, cpus-1)
	for _, chunk := range chunks {
		go func(chunk []byte) {
			processLines(chunk, out)
		}(chunk)
	}

	for i := 0; i < cpus; i++ {
		for k, v := range <-out {
			if d, ok := results[k]; !ok {
				results[k] = v
			} else {
				d.min = math.Min(d.min, v.min)
				d.max = math.Max(d.max, v.max)
				d.numPoints += v.numPoints
				d.total += v.total
			}
		}
	}

	close(out)

	for k, v := range results {
		fmt.Printf("{ \"station\": \"%s\", \"min\": %.2f, \"max\": %.2f, \"avg\": %.2f }\n", k, v.min, v.max, v.total/float64(v.numPoints))
	}

	readerAt.Close()
	fmt.Println("elapsed: ", time.Since(start))
}

func chunkBytes(b []byte, amt int) [][]byte {
	// amt must be shorter than number of lines in the csv

	chunks := make([][]byte, 0)
	blen := len(b)

	start := 0
	end := 0
	size := blen / amt

	for start < blen {
		maxIndex := start + size
		if maxIndex < blen-1 {
			idx := bytes.LastIndexByte(b[start:maxIndex], '\n')
			end += idx
		} else {
			end = blen
		}
		chunks = append(chunks, b[start:end])
		start = end + 1
	}

	return chunks
}

func processLines(lines []byte, out chan map[string]*data) {
	m := map[string]*data{}

	var lineEnd, lineStart int
	llen := len(lines)

	for lineEnd < llen {
		lineEnd = bytes.IndexByte(lines[lineStart:], '\n')
		if lineEnd == -1 {
			lineEnd = llen
		} else {
			lineEnd += lineStart + 1
		}

		line := string(lines[lineStart : lineEnd-1])
		if line == "" {
			lineStart = lineEnd
			continue
		}

		// Index the ';' for the given line, so we can use it to manually split the line into name and val
		sep := strings.Index(line, ";")
		if sep == -1 {
			panic("sep -1")
		}

		name := line[:sep]
		// lineEnd -1 in order to avoid the \n at the end
		val, err := strconv.ParseFloat(line[sep+1:], 64)
		if err != nil {
			panic(err)
		}

		d, ok := m[name]
		if !ok {
			m[name] = &data{
				min:       val,
				max:       val,
				total:     val,
				numPoints: 1,
			}
		} else {
			d.min = math.Min(d.min, val)
			d.max = math.Max(d.max, val)

			d.numPoints += 1
			d.total += val
		}

		// move start index to next line
		lineStart = lineEnd
	}

	out <- m
}
