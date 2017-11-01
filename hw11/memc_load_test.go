package main

import (
	"bufio"
	"github.com/AndrewLrrr/memclog/appsinstalled"
	"github.com/golang/protobuf/proto"
	"log"
	"strings"
	"testing"
)

func TestBufLine(t *testing.T) {
	lines := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t56.55\t43.42\t7423,424"

	scanner := bufio.NewScanner(strings.NewReader(lines))
	for scanner.Scan() {
		logLine, err := parseLine(scanner.Text())

		if err != nil {
			t.Error(err)
		} else {
			key, test, err := protobufLine(logLine)
			if err != nil {
				t.Error(err)
			}
			newTest := &appsinstalled.UserApps{}
			err = proto.Unmarshal(test, newTest)

			if err != nil {
				t.Error("unmarshaling error: ", err)
			}

			if key != logLine.devType+":"+logLine.devId {
				t.Error("Incorrect key")
			}

			if logLine.lon != *newTest.Lon {
				t.Error("Incorrect longitude")
			}

			if logLine.lat != *newTest.Lat {
				t.Error("Incorrect latitude")
			}

			for i := range logLine.apps {
				if logLine.apps[i] != newTest.Apps[i] {
					t.Error("Incorrect apps")
					break
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
