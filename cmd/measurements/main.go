package main

import (
	"bytes"
	"fmt"
	"golang.org/x/exp/mmap"
	"strings"
	"time"
)

type data struct {
	min float32
	max float32

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
				d.min = f32Min(d.min, v.min)
				d.max = f32Max(d.max, v.max)
				d.numPoints += v.numPoints
				d.total += v.total
			}
		}
	}

	close(out)

	for k, v := range results {
		fmt.Printf("{ \"station\": \"%s\", \"min\": %.1f, \"max\": %.1f, \"avg\": %.1f }\n", k, v.min, v.max, v.total/float64(v.numPoints))
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
		val, err := parseFloat32(line[sep+1:])
		if err != nil {
			panic(err)
		}

		d, ok := m[name]
		if !ok {
			m[name] = &data{
				min:       val,
				max:       val,
				total:     float64(val),
				numPoints: 1,
			}
		} else {
			d.min = f32Min(d.min, val)
			d.max = f32Max(d.max, val)

			d.numPoints += 1
			d.total += float64(val)
		}

		// move start index to next line
		lineStart = lineEnd
	}

	out <- m
}

func f32Min(x, y float32) float32 {
	if x < y {
		return x
	}
	return y
}

func f32Max(x, y float32) float32 {
	if x > y {
		return x
	}
	return y
}

// I'm not smart enough to understand go's implementation but this works for our use case
func parseFloat32(s string) (float32, error) {
	var (
		f         float32
		div       float32 = 10 // anything after '.' causes a division by at least 10
		isDecimal bool
		negative  bool
	)

	for i, char := range s {
		if char >= '0' && char <= '9' {
			if !isDecimal {
				f *= 10 // every non decimal multiplies by 10
				f += float32(char - '0')
			} else {
				f += float32(char-'0') / div
				div *= 10 // every next decimal divides by *10
			}
		} else if char == '.' {
			isDecimal = true
		} else if char == '-' && i == 0 {
			negative = true
		} else {
			return 0, fmt.Errorf("this ain't gonna float: %s", s)
		}
	}

	if negative {
		f *= -1
	}

	return f, nil
}
