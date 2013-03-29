package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/skydb/sky.go"
	"runtime"
	"os"
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
	flag.StringVar(&host, "data-dir", defaultHost, hostUsage)
	flag.StringVar(&host, "d", defaultHost, hostUsage+" (shorthand)")
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
	// Parse the command line arguments.
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Setup the client and table.
	_, _, err := setup()
	if err != nil {
		warn("%v", err)
		return
	}
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
		} else {
			return nil, nil, errors.New("Table already exists. Use --overwrite to overwrite it.")
		}
	}

	// Create the table.
	table = sky.NewTable(tableName, client)
	if err = client.CreateTable(table); err != nil {
		return nil, nil, err
	}

	// Add properties.
	properties := []*sky.Property{
		sky.NewProperty("username", false, sky.String),
		sky.NewProperty("company", false, sky.String),
		sky.NewProperty("action", true, sky.Factor),
		sky.NewProperty("subaction", true, sky.Factor),
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

	return client, table, nil
}

//--------------------------------------
// Utility
//--------------------------------------

// Writes to standard error.
func warn(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}
