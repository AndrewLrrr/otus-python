package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type LogLine struct {
	devType, devId string
	lat, lon       float64
	apps           []uint32
}

func dotRename(path string) error {
	newPath := "." + path
	return os.Rename(path, newPath)
}

func (logLine *LogLine) bufLine() error {
	return nil
}

func (logLine *LogLine) parseLine(line string) error {
	lineParts := strings.Split(strings.TrimSpace(line), "\t")

	if len(lineParts) != 5 {
		return errors.New("incorrect log line\n")
	}

	logLine.devType = strings.TrimSpace(lineParts[0])
	logLine.devId = strings.TrimSpace(lineParts[1])

	if len(logLine.devId) == 0 {
		return errors.New("empty dev_id is not allowed\n")
	}

	if len(logLine.devType) == 0 {
		return errors.New("empty dev_type is not allowed\n")
	}

	if lat, err := strconv.ParseFloat(lineParts[2], 64); err == nil {
		logLine.lat = lat
	} else {
		logLine.lat = 0
	}

	if lon, err := strconv.ParseFloat(lineParts[3], 64); err == nil {
		logLine.lon = lon
	} else {
		logLine.lon = 0
	}

	apps := strings.Split(strings.TrimSpace(lineParts[4]), ",")

	for _, app := range apps {
		if num, err := strconv.ParseUint(app, 10, 32); err == nil {
			logLine.apps = append(logLine.apps, uint32(num))
		}
	}

	return nil
}

func main() {
	line := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23"

	logLine := LogLine{}

	err := logLine.parseLine(line)
	if err != nil {
		fmt.Printf("%s\n", err)
	} else {
		fmt.Print(logLine)
	}
}
