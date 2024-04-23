package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	stationCount           = 40
	measurementsPerStation = 250000
)

type measurement struct {
	station string
	temp    float64
}

func main() {
	allStations, err := readWeatherStations("../../data/worldcities.csv")
	if err != nil {
		panic(err)
	}

	stations := pickRandom(allStations, stationCount)

	mch := make(chan measurement)
	go appendToData("../../data/measurements.csv", mch)

	var wg sync.WaitGroup
	for _, s := range stations {
		wg.Add(1)
		go generateStationMeasurements(&wg, s, measurementsPerStation, mch)
	}

	wg.Wait()
	close(mch)
	time.Sleep(time.Second)
}

func generateStationMeasurements(wg *sync.WaitGroup, station string, n uint, data chan<- measurement) {
	defer wg.Done()
	fmt.Printf("generating %d measurements for %s\n", n, station)
	for i := 0; i < int(n); i++ {
		temp := (50 * rand.Float64()) - 10.0
		data <- measurement{station: station, temp: temp}
	}
}

func readWeatherStations(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(f)
	r.Comment = '#'

	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, errors.New("no weatherstations found")
	}

	stations := make([]string, len(records))
	for i, r := range records {
		stations[i] = r[0]
	}

	return stations, nil
}

func pickRandom(stations []string, n uint) []string {
	picks := make([]string, n)
	for i := 0; i < int(n); i++ {
		rndIdx := rand.Intn(len(stations))
		picks[i] = stations[rndIdx]
	}
	return picks
}

func appendToData(path string, data <-chan measurement) {
	var lines uint
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer func() {
		f.Sync()
		f.Close()
	}()

	for m := range data {
		fmt.Fprintf(f, "%s;%.4f\n", m.station, m.temp)
		lines++
	}

	fmt.Printf("wrote %d measurements to %s\n", lines, path)
}
