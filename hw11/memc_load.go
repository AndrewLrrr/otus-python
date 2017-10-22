package main

import (
	"github.com/golang/protobuf/proto"
	"github.com/AndrewLrrr/memclog/appsinstalled"
	"errors"
	"os"
	"strconv"
	"strings"
	"log"
	"fmt"
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

func bufLine(logLine LogLine) (string, []byte, error) {
	packed := appsinstalled.UserApps{
		Apps: logLine.apps,
		Lat:  &logLine.lat,
		Lon:  &logLine.lon,
	}
	key := fmt.Sprintf("%s:%s", logLine.devType, logLine.devId)

	data, err := proto.Marshal(&packed)
	if err != nil {
		return "", data, errors.New(fmt.Sprintf("marshaling error: %s", err))
	}

	return key, data, nil
}

func (logLine *LogLine) parse(line string) error {
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
		log.Printf("invalid latitude: `%s`\n", line)
	}

	if lon, err := strconv.ParseFloat(lineParts[3], 64); err == nil {
		logLine.lon = lon
	} else {
		logLine.lon = 0
		log.Printf("invalid longitude: `%s`\n", line)
	}

	apps := strings.Split(strings.TrimSpace(lineParts[4]), ",")

	for _, app := range apps {
		if num, err := strconv.ParseUint(app, 10, 32); err == nil {
			logLine.apps = append(logLine.apps, uint32(num))
		} else {
			log.Printf("not all user apps are digits: `%s`\n", line)
		}
	}

	return nil
}

func main() {
}
