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
	"runtime"
	"os"
	"time"
)

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
	defaultHost      = "localhost"
	defaultPort      = 8585
	defaultTableName = "gharchive"
	defaultOverwrite = false
)

const (
	hostUsage      = "the host the Sky server is running on"
	portUsage      = "the port the Sky server is running on"
	tableNameUsage = "the table to insert events into"
	overwriteUsage = "overwrite an existing table if one exists"
)

//------------------------------------------------------------------------------
//
// Variables
//
//------------------------------------------------------------------------------

var host string
var port int
var tableName string
var overwrite bool

//------------------------------------------------------------------------------
//
// Functions
//
//------------------------------------------------------------------------------

//--------------------------------------
// Initialization
//--------------------------------------

func init() {
	flag.StringVar(&host, "host", defaultHost, hostUsage)
	flag.StringVar(&host, "h", defaultHost, hostUsage+" (shorthand)")
	flag.IntVar(&port, "port", defaultPort, portUsage)
	flag.IntVar(&port, "p", defaultPort, portUsage+" (shorthand)")
	flag.StringVar(&tableName, "table", defaultTableName, tableNameUsage)
	flag.StringVar(&tableName, "t", defaultTableName, tableNameUsage+" (shorthand)")
	flag.BoolVar(&overwrite, "overwrite", defaultOverwrite, overwriteUsage)
}

//--------------------------------------
// Main
//--------------------------------------

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
	
	// Loop over date range.
	hours := int(endDate.Sub(startDate) / time.Hour) + 1
	for i := 0; i < hours; i++ {
		date := startDate.Add(time.Duration(i) * time.Hour)
		if err = importDate(table, date); err != nil {
			warn("%v", err)
			os.Exit(1)
		}
	}
}

func usage() {
	warn("usage: sky-gha-importer [OPTIONS] START_DATE END_DATE")
	os.Exit(1)
}

//--------------------------------------
// Setup
//--------------------------------------

func setup() (*sky.Client, *sky.Table, error) {
	warn("Connecting to %s:%d.\n", host, port)
	
	// Create a Sky client.
	client := sky.NewClient(host)
	client.Port = port

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

//--------------------------------------
// Setup
//--------------------------------------

// Imports GitHub Archive data for a given hour.
func importDate(table *sky.Table, date time.Time) error {
	// Retrieve gziped JSON file.
	url := fmt.Sprintf("http://data.githubarchive.org/%d-%02d-%02d-%d.json.gz", date.Year(), int(date.Month()), date.Day(), date.Hour())
	warn("%v", url)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	// Decompress response.
	gzipReader, err := gzip.NewReader(resp.Body)
	defer gzipReader.Close()
	r := bufio.NewReader(gzipReader)
	lineNumber := 0
	for {
		lineNumber += 1
		
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Parse data from the stream.
		data := map[string]interface{}{}
		if err = json.Unmarshal(line, &data); err != nil {
			return err
		}
		
		// Create an event.
		if timestampString, ok := data["created_at"].(string); ok {
			if timestamp, err := time.Parse(time.RFC3339, timestampString); err == nil {
				if username, ok := data["actor"].(string); ok && len(username) > 0 {
					event := sky.NewEvent(timestamp, map[string]interface{}{})
				
					if repository, ok := data["repository"].(map[string]interface{}); ok {
						event.Data["language"] = repository["language"]
						event.Data["forks"] = repository["forks"]
						event.Data["watchers"] = repository["watchers"]
						event.Data["stargazers"] = repository["stargazers"]
						event.Data["size"] = repository["size"]
					}
			
					table.AddEvent(username, event, sky.Merge)
				} else {
					warn("[L%d] Actor required", lineNumber)
				}
			} else {
				warn("[L%d] Invalid timestamp: %v (%v)", lineNumber, timestampString, err)
			}
		} else {
			warn("[L%d] Timestamp required.", lineNumber)
		}
	}
	
	return nil
}

//--------------------------------------
// Utility
//--------------------------------------

// Writes to standard error.
func warn(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}
