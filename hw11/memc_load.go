package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/AndrewLrrr/memclog/appsinstalled"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"strconv"
	"strings"
)

type LogLine struct {
	devType, devId string
	lat, lon       float64
	apps           []uint32
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

type Memcache struct {
	connection *memcache.Client
}

func (mc *Memcache) setItem(key string, value []byte) error {
	return mc.connection.Set(&memcache.Item{Key: key, Value: value})
}

func dotRename(path string) error {
	newPath := "." + path
	return os.Rename(path, newPath)
}

func setConnection(addr string) *Memcache {
	mc := Memcache{}
	mc.connection = memcache.New(addr)
	return &mc
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

func main() {
	filename := "20170929000000.tsv.gz"

	connections := map[string]*Memcache{
		"idfa": setConnection("127.0.0.1:33013"),
		"gaid": setConnection("127.0.0.1:33014"),
		"adid": setConnection("127.0.0.1:33015"),
		"dvid": setConnection("127.0.0.1:33016"),
	}

	file, err := os.Open(filename)

	if err != nil {
		log.Fatal(err)
	}

	gz, err := gzip.NewReader(file)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()
	defer gz.Close()

	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		logLine := LogLine{}
		err := logLine.parse(scanner.Text())
		if err != nil {
			log.Println(err)
		}

		key, parsed, err := bufLine(logLine)
		if err != nil {
			log.Println(err)
		} else {
			connection := connections[logLine.devType]
			err := connection.setItem(key, parsed)
			if err != nil {
				log.Println(err)
			}
		}
	}

	if err = scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
