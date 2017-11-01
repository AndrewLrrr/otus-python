package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/AndrewLrrr/memclog/appsinstalled"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	LineWorkers     = 100
	ChannelsBuffer  = 100
	LogsDir         = "./logs"
	MemcacheTimeout = 500 * time.Millisecond
	NormalErrRate   = 0.01
)

type LogLine struct {
	devType, devId string
	lat, lon       float64
	apps           []uint32
}

type MemcacheTask struct {
	key   string
	value []byte
}

type Statistic struct {
	processed int
	errors    int
}

type Memcache struct {
	connection *memcache.Client
}

func NewMemcache(addr string) *Memcache {
	mc := Memcache{}
	mc.connection = memcache.New(addr)
	mc.connection.Timeout = MemcacheTimeout
	return &mc
}

func (mc *Memcache) setItem(key string, value []byte) error {
	tries := 3
	delay := 100
	backoff := 2
	if err := mc.connection.Set(&memcache.Item{Key: key, Value: value}); err != nil {
		for {
			time.Sleep(time.Duration(delay) * time.Millisecond)
			if err != nil && tries > 0 {
				err = mc.connection.Set(&memcache.Item{Key: key, Value: value})
				tries--
				delay *= backoff
			} else {
				return err
			}
		}
	}
	return nil
}

func lineWorker(channels map[string](chan *MemcacheTask), queue <-chan string, stats chan *Statistic) {
	processErrors := 0
	for {
		line, ok := <-queue
		if !ok {
			stat := &Statistic{
				processed: 0,
				errors:    processErrors,
			}
			stats <- stat
			return
		}
		logLine, err := parseLine(line)
		if err != nil {
			processErrors++
			log.Println(err)
		} else {
			packed, err := protobufLine(logLine)
			if err != nil {
				processErrors++
				log.Println(err)
			} else {
				key := fmt.Sprintf("%s:%s", logLine.devType, logLine.devId)
				task := &MemcacheTask{
					key:   key,
					value: packed,
				}
				channels[logLine.devType] <- task
			}
		}
	}
}

func memcacheWorker(mc *Memcache, queue <-chan *MemcacheTask, stats chan *Statistic) {
	processSuccess := 0
	processErrors := 0
	for {
		task, ok := <-queue
		if !ok {
			stat := &Statistic{
				processed: processSuccess,
				errors:    processErrors,
			}
			stats <- stat
			log.Printf("memcache processed %d | errors %d", processSuccess, processErrors)
			return
		}
		err := mc.setItem(task.key, task.value)
		if err != nil {
			processErrors++
			log.Println(err)
		} else {
			processSuccess++
		}
	}
}

func dotRename(path string) error {
	if name := filepath.Base(path); !strings.HasPrefix(name, ".") {
		newName := "." + name
		dir := filepath.Dir(path)
		newPath := filepath.Join(dir, newName)
		return os.Rename(path, newPath)
	}
	return nil
}

func parseLine(line string) (LogLine, error) {
	lineParts := strings.Split(strings.TrimSpace(line), "\t")
	logLine := LogLine{}

	if len(lineParts) != 5 {
		return logLine, errors.New("incorrect log line")
	}

	logLine.devType = strings.TrimSpace(lineParts[0])
	logLine.devId = strings.TrimSpace(lineParts[1])

	if len(logLine.devId) == 0 {
		return logLine, errors.New("empty dev_id is not allowed")
	}

	if len(logLine.devType) == 0 {
		return logLine, errors.New("empty dev_type is not allowed")
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

	return logLine, nil
}

func protobufLine(logLine LogLine) ([]byte, error) {
	packed := appsinstalled.UserApps{
		Apps: logLine.apps,
		Lat:  &logLine.lat,
		Lon:  &logLine.lon,
	}

	data, err := proto.Marshal(&packed)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("marshaling error: %s", err))
	}

	return data, nil
}

func fileHandler(filePath string, connections map[string]*Memcache, statistic chan *Statistic) (map[string]int, error) {
	channels := make(map[string](chan *MemcacheTask))
	for key, connection := range connections {
		channels[key] = make(chan *MemcacheTask, ChannelsBuffer)
		go memcacheWorker(connection, channels[key], statistic)
	}

	read := 0
	lines := make(chan string, ChannelsBuffer)

	for i := 0; i < LineWorkers; i++ {
		go lineWorker(channels, lines, statistic)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	gz, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		lines <- scanner.Text()
		read += 1
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	defer gz.Close()
	defer file.Close()

	close(lines)

	result := map[string]int{
		"read": read,
		"processSuccess": 0,
		"processErrors": 0,
	}

	for j := 0; j < LineWorkers; j++ {
		stat := <-statistic
		result["processSuccess"] += stat.processed
		result["processErrors"] += stat.errors
	}

	for _, channel := range channels {
		close(channel)
		stat := <-statistic
		result["processSuccess"] += stat.processed
		result["processErrors"] += stat.errors
	}

	return result, nil
}

func main() {
	num := runtime.NumCPU()
	runtime.GOMAXPROCS(num)

	files, err := ioutil.ReadDir(LogsDir)
	if err != nil {
		log.Fatal(err)
	}

	connections := map[string]*Memcache{
		"idfa": NewMemcache("127.0.0.1:33013"),
		"gaid": NewMemcache("127.0.0.1:33014"),
		"adid": NewMemcache("127.0.0.1:33015"),
		"dvid": NewMemcache("127.0.0.1:33016"),
	}

	statistic := make(chan *Statistic)
	filePaths := []string{}

	for _, file := range files {
		r, err := regexp.MatchString("^[^\\.]+\\.tsv\\.gz$", file.Name())
		if err == nil && r {
			filePaths = append(filePaths, filepath.Join(LogsDir, file.Name()))
		}
	}

	sort.Strings(filePaths)

	totalRead := 0
	totalProcessed := 0
	totalErrors := 0
	for _, filePath := range filePaths {
		log.Printf("start handle file %s", filePath)

		result, err := fileHandler(filePath, connections, statistic)

		if err != nil {
			log.Fatal(err)
		}

		dotRename(filePath)
		log.Printf("finish handle file %s", filePath)

		totalRead += result["read"]
		totalProcessed += result["processSuccess"]
		totalErrors += result["processErrors"]
		log.Printf("read lines: %d", result["read"])
		log.Printf("processed lines: %d", result["processSuccess"])
		log.Printf("error lines: %d", result["processErrors"])

		if result["processSuccess"] > 0 {
			errRate := float64(result["processErrors"] / result["processSuccess"])
			if errRate < NormalErrRate {
				log.Printf("file: %s | Acceptable error rate (%f). Successfull load", filePath, errRate)
			} else {
				log.Printf("file: %s | High error rate (%f > %f). Failed load", filePath, errRate, NormalErrRate)
			}
		}
		log.Println("----------------------------------------------")
	}

	close(statistic)

	log.Printf("total read lines: %d", totalRead)
	log.Printf("total processed lines: %d", totalProcessed)
	log.Printf("total error lines: %d", totalErrors)
}
