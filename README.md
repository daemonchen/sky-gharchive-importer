# Sky GitHub Archive Importer

## Overview

This importer is for pulling public [GitHub events](http://developer.github.com/v3/activity/events/) into the Sky database via the [GitHub Archive](http://www.githubarchive.org/).
These events consist of commits, pushes, repository creation and more.
You can find a full list in the [GitHub Event Type](http://developer.github.com/v3/activity/events/types/) documentation.
Archives are built every hour so you can specify a time range to load.

## Installation

To build the binary for the importer, first make sure you have [Go](http://golang.org/) installed and then run:

```sh
$ go build
```

You should see a `sky-gharchive-importer` binary available in your current directory.


## Usage

To run the Sky GitHub Archive importer, make sure you have Sky running first and then run the following command:

```sh
# Import a single hour of GitHub data.
$ ./sky-gharchive-importer 2013-01-01T00:00:00Z
```

```sh
# Import a date range of GitHub data.
$ ./sky-gharchive-importer 2013-01-01T00:00:00Z 2013-01-31T23:00:00Z
```

By default the importer will append to the `gharchive` table on a Sky instance running locally.
You can also override this by specifying the following options:

```sh
-h, --host HOST    The hostname that Sky is running on (defaults to 'localhost').
-p, --port PORT    The port number Sky is running on (defaults to 8585).
-t, --table TABLE  The table name to insert into (defaults to 'gharchive').
--overwrite        Deletes the table if it already exists.
-v,--verbose       Enables verbose logging.
```

The GitHub Archive data is not necessarily sequential so you may find that Sky slows down considerably at some points because the database is optimized appends and not for random inserts


## Questions & Bugs

If you have any questions or bugs, please send an e-mail to the [Sky Google Group](https://groups.google.com/d/forum/skydb). 