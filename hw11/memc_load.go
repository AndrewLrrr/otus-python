package main

import (
	"github.com/golang/protobuf/proto"
	"./appsinstalled"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"bufio"
	"log"
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

func bufLine(logLine LogLine) (string, appsinstalled.UserApps) {
	data := appsinstalled.UserApps{
		Apps: logLine.apps,
		Lat:  &logLine.lat,
		Lon:  &logLine.lon,
	}
	key := strings.Join([]string{logLine.devType, logLine.devId}, ":")

	return key, data
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
		log.Printf("invalid latitude: `%s`", line)
	}

	if lon, err := strconv.ParseFloat(lineParts[3], 64); err == nil {
		logLine.lon = lon
	} else {
		logLine.lon = 0
		log.Printf("invalid longitude: `%s`", line)
	}

	apps := strings.Split(strings.TrimSpace(lineParts[4]), ",")

	for _, app := range apps {
		if num, err := strconv.ParseUint(app, 10, 32); err == nil {
			logLine.apps = append(logLine.apps, uint32(num))
		} else {
			log.Printf("not all user apps are digits: `%s`", line)
		}
	}

	return nil
}

func protoTest() {
	lines := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"

	scanner := bufio.NewScanner(strings.NewReader(lines))
	for scanner.Scan() {
		logLine := LogLine{}
		err := logLine.parse(scanner.Text())

		if err != nil {
			fmt.Printf("%s\n", err)
		} else {
			fmt.Print(logLine)
			fmt.Print("\n")
			_, packed := bufLine(logLine)
			data, err := proto.Marshal(&packed)
			if err != nil {
				log.Fatal("marshaling error: ", err)
			}
			newTest := &appsinstalled.UserApps{}
			err = proto.Unmarshal(data, newTest)
			if err != nil {
				log.Fatal("unmarshaling error: ", err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	protoTest()
}
