package main

import (
	"fmt"
	"golang.org/x/exp/mmap"
	"runtime"
	"testing"
)

func TestChunkBytes(t *testing.T) {
	ra, err := mmap.Open("../../data/measurements.csv")
	if err != nil {
		t.Fatal(err)
	}

	//for i := 0; i < 3; i++ {
	b := make([]byte, ra.Len())
	ra.ReadAt(b, 0)
	//}

	// expect 3 sets of lines
	chunks := chunkBytes(b, runtime.NumCPU()-1)

	fmt.Println(len(chunks))
	fmt.Println(string(chunks[30]))
}
