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

	b := make([]byte, ra.Len())
	ra.ReadAt(b, 0)

	chunks := chunkBytes(b, runtime.NumCPU()-1)

	fmt.Println(len(chunks))
	fmt.Println(string(chunks[31]))
}

func TestParseFloat32(t *testing.T) {
	str := "32.367904"
	exp := float32(32.367904)

	f, err := parseFloat32(str)
	if err != nil {
		t.Error(err)
	}
	if f != exp {
		t.Errorf("result doesnt match, got: %v, exp: %v", f, exp)
	}
}
