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
	LogsDir         = "./logs"
	MemcacheTimeout = 500 * time.Millisecond
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

func dotRename(path string) error {
	if name := filepath.Base(path); !strings.HasPrefix(name, ".") {
		newName := "." + name
		dir := filepath.Dir(path)
		newPath := filepath.Join(dir, newName)
		return os.Rename(path, newPath)
	}
	return nil
}

func setConnection(addr string) *Memcache {
	mc := Memcache{}
	mc.connection = memcache.New(addr)
	mc.connection.Timeout = MemcacheTimeout
	return &mc
}

func parseLine(line string) (LogLine, error) {
	lineParts := strings.Split(strings.TrimSpace(line), "\t")
	logLine := LogLine{}

	if len(lineParts) != 5 {
		return logLine, errors.New("incorrect log line\n")
	}

	logLine.devType = strings.TrimSpace(lineParts[0])
	logLine.devId = strings.TrimSpace(lineParts[1])

	if len(logLine.devId) == 0 {
		return logLine, errors.New("empty dev_id is not allowed\n")
	}

	if len(logLine.devType) == 0 {
		return logLine, errors.New("empty dev_type is not allowed\n")
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

	return logLine, nil
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
			key, packed, err := bufLine(logLine)
			if err != nil {
				processErrors++
				log.Println(err)
			} else {
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

func main() {
	num := runtime.NumCPU()
	runtime.GOMAXPROCS(num)

	files, err := ioutil.ReadDir(LogsDir)
	if err != nil {
		log.Fatal(err)
	}

	connections := map[string]string{
		"idfa": "127.0.0.1:33013",
		"gaid": "127.0.0.1:33014",
		"adid": "127.0.0.1:33015",
		"dvid": "127.0.0.1:33016",
	}

	channels := make(map[string](chan *MemcacheTask))
	statistic := make(chan *Statistic)

	for key, addr := range connections {
		channels[key] = make(chan *MemcacheTask, LineWorkers)
		go memcacheWorker(setConnection(addr), channels[key], statistic)
	}

	filePaths := []string{}

	for _, file := range files {
		r, err := regexp.MatchString("^[^\\.]+\\.tsv\\.gz$", file.Name())
		if err == nil && r {
			filePaths = append(filePaths, filepath.Join(LogsDir, file.Name()))
		}
	}

	lines := make(chan string, LineWorkers)

	for i := 0; i < LineWorkers; i++ {
		go lineWorker(channels, lines, statistic)
	}

	sort.Strings(filePaths)

	read := 0
	for _, filePath := range filePaths {
		log.Printf("start handle file %s", filePath)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatal(err)
		}

		gz, err := gzip.NewReader(file)
		if err != nil {
			log.Fatal(err)
		}

		scanner := bufio.NewScanner(gz)

		for scanner.Scan() {
			lines <- scanner.Text()
			read++
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

		gz.Close()
		file.Close()

		dotRename(filePath)
		log.Printf("finish handle file %s", filePath)
	}

	close(lines)

	processSuccess := 0
	processErrors := 0
	for j := 0; j < LineWorkers; j++ {
		stat := <-statistic
		processSuccess += stat.processed
		processErrors += stat.errors
	}

	for _, channel := range channels {
		close(channel)
		stat := <-statistic
		processSuccess += stat.processed
		processErrors += stat.errors
	}

	log.Printf("total read lines: %d", read)
	log.Printf("total processed lines: %d", processSuccess)
	log.Printf("total error lines: %d", processErrors)
}
