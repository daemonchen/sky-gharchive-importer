package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/skydb/sky.go"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"time"
)

const (
	Version = "0.3.0"
)

var host string
var port uint
var tableName string
var overwrite bool
var verbose bool

func init() {
	flag.StringVar(&host, "h", "localhost", "the host the Sky server is running on")
	flag.UintVar(&port, "p", 8585, "the port the Sky server is running on")
	flag.StringVar(&tableName, "t", "gharchive", "the table to insert events into")
	flag.BoolVar(&overwrite, "overwrite", false, "overwrite an existing table if one exists")
	flag.BoolVar(&verbose, "v", false, "enable verbose logging")
}

func main() {
	var err error

	// Parse the command line arguments.
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Parse start and end date.
	var startDate, endDate time.Time
	if flag.NArg() == 0 {
		usage()
	} else if flag.NArg() == 1 {
		if startDate, err = time.Parse(time.RFC3339, flag.Arg(0)); err != nil {
			warn("Invalid start date: %s", flag.Arg(0))
			os.Exit(1)
		}
		endDate = startDate
	} else {
		if startDate, err = time.Parse(time.RFC3339, flag.Arg(0)); err != nil {
			warn("Invalid start date: %s", flag.Arg(0))
			os.Exit(1)
		}
		if endDate, err = time.Parse(time.RFC3339, flag.Arg(1)); err != nil {
			warn("Invalid end date: %s", flag.Arg(1))
			os.Exit(1)
		}
	}

	// Setup the client and table.
	_, table, err := setup()
	if err != nil {
		warn("%v", err)
		os.Exit(1)
	}

	// Send events on a separate stream.
	c := make(chan UserEvents, 5)
	go stream(table, c)

	// Loop over date range.
	hours := int(endDate.Sub(startDate)/time.Hour) + 1
	for i := 0; i < hours; i++ {
		date := startDate.Add(time.Duration(i) * time.Hour)
		if err = getRawData(date, c); err != nil {
			warn("Invalid file: %v", err)
		}
	}
}

func usage() {
	warn("usage: sky-gha-importer [OPTIONS] START_DATE END_DATE")
	os.Exit(1)
}

func setup() (sky.Client, sky.Table, error) {
	warn("Connecting to %s:%d.\n", host, port)

	// Create a Sky client.
	client := sky.NewClient(host)
	client.SetPort(port)

	// Check if the server is running.
	if !client.Ping() {
		return nil, nil, errors.New("Server is not running.")
	}

	// Check if the table exists first.
	table, err := client.GetTable(tableName)
	if table != nil {
		if overwrite {
			err = client.DeleteTable(table)
			if err != nil {
				return nil, nil, err
			}
			table = nil
		}
	}

	if table == nil {
		// Create the table.
		table = sky.NewTable(tableName, client)
		if err = client.CreateTable(table); err != nil {
			return nil, nil, err
		}

		// Add properties.
		properties := []*sky.Property{
			sky.NewProperty("username", false, sky.String),
			sky.NewProperty("action", true, sky.Factor),
			sky.NewProperty("language", true, sky.Factor),
			sky.NewProperty("forks", true, sky.Integer),
			sky.NewProperty("watchers", true, sky.Integer),
			sky.NewProperty("stargazers", true, sky.Integer),
			sky.NewProperty("size", true, sky.Integer),
		}
		for _, property := range properties {
			if err = table.CreateProperty(property); err != nil {
				return nil, nil, err
			}
		}
	}

	return client, table, nil
}

// getRawData retrieves the events for a given hour and sends them to a channel.
func getRawData(date time.Time, c chan UserEvents) error {
	// Retrieve gziped JSON file.
	url := fmt.Sprintf("http://data.githubarchive.org/%d-%02d-%02d-%d.json.gz", date.Year(), int(date.Month()), date.Day(), date.Hour())
	warn("%v", url)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	events := []*UserEvent{}

	// Decompress response.
	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}
	defer gzipReader.Close()
	r := bufio.NewReader(gzipReader)
	decoder := json.NewDecoder(r)
	lineNumber := 0
	for {
		lineNumber += 1

		// Parse data from the stream.
		data := map[string]interface{}{}
		if err = decoder.Decode(&data); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("[L%d] %v", lineNumber, err)
		} else {
			// Create an event.
			if timestampString, ok := data["created_at"].(string); ok {
				if timestamp, err := time.Parse(time.RFC3339, timestampString); err == nil {
					if username, ok := data["actor"].(string); ok && len(username) > 0 {
						event := sky.NewEvent(timestamp, map[string]interface{}{})
						event.Data["action"] = data["type"]

						if repository, ok := data["repository"].(map[string]interface{}); ok {
							event.Data["language"] = repository["language"]
							event.Data["forks"] = repository["forks"]
							event.Data["watchers"] = repository["watchers"]
							event.Data["stargazers"] = repository["stargazers"]
							event.Data["size"] = repository["size"]
						}

						events = append(events, &UserEvent{username: username, event: event})
					} else if verbose {
						warn("[L%d] Actor required", lineNumber)
					}
				} else if verbose {
					warn("[L%d] Invalid timestamp: %v (%v)", lineNumber, timestampString, err)
				}
			} else if verbose {
				warn("[L%d] Timestamp required.", lineNumber)
			}
		}
	}

	// Sort events by timestamp.
	sort.Sort(UserEvents(events))
	c <- events

	return nil
}

// stream reads from a channel and continuously pipes new events to Sky.
func stream(t sky.Table, c chan UserEvents) {
	for {
		events := <- c
		t.Stream(func(stream *sky.EventStream) {
			for i, e := range events {
				if err := stream.AddEvent(e.username, e.event); err != nil {
					warn("[L%d] Unable to add event", i+1)
				}
			}
		})
	}
}

// UserEvent temporarily stores event data from the Github Archive stream.
type UserEvent struct {
	username string
	event    *sky.Event
}

type UserEvents []*UserEvent

func (s UserEvents) Len() int {
	return len(s)
}

func (s UserEvents) Less(i, j int) bool {
	return s[i].event.Timestamp.Before(s[j].event.Timestamp)
}

func (s UserEvents) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Writes to standard error.
func warn(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}