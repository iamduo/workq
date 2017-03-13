# Command log

Command log is an append-only data log of all job commands that perform state changes. It is used for persistence and recovery of a workq-server.

**How it works**

A successful job command is logged to capture its intent and is logged in an optimized form of the original [command](protocol.md). This includes any background events such as job expirations from the "add" command.

Job commands are logged in the same order as they are processed. During server startup, the logs are replayed in the same order before it starts accepting new commands, thus restoring the original jobs dataset.

Periodically the command logs are [cleaned](#cleaning-cycle-strategy) in the background to remove jobs that have expired passed their TTL. The interval of cleaning can be configured through the `cmdlog-clean-int` option.

For more detailed information, please refer to the [Internals](#Internals) section.

**Table of Contents**

- [Command log](#command-log)
  - [Usage](#usage)
  - [Configuring](#configuring)
    - [cmdlog-path](#cmdlog-path)
    - [cmdlog-sync](#cmdlog-sync)
    - [cmdlog-sync-int](#cmdlog-sync-int)
    - [cmdlog-clean-int](#cmdlog-clean-int)
    - [cmdlog-seg-size](#cmdlog-seg-size)
  - [Internals](#internals)
    - [Log Directory Structure](#log-directory-structure)
    - [Log File Format](#log-file-format)
      - [Magic header](#magic-header)
      - [Record Format](#record-format)
      - [Command Serialization Format](#command-serialization-format)
        - [add, schedule](#add-schedule)
          - [Time Format](#time-format)
        - [complete, fail, delete](#complete-fail-delete)
    - [Cleaning Cycle Strategy](#cleaning-cycle-strategy)
      - [Rewrite Process](#rewrite-process)

## Usage

The command log can be enabled by setting the `cmdlog-path` when starting Workq. The path is a directory where the data logs are written as rolling segment files.

```text
workq-server -cmdlog-path /path/to/workq/cmdlog-dir
```

Command logging starts up with defaults and can be fined tuned using the flags described in [Configuring](#configuring). Defaults are set to [64MiB segment files](#cmdlog-seg-size) [disk syncing every second](#cmdlog-sync-int) with [cleaning every 5 minutes](#cmdlog-clean-int).

## Configuring

The following describes `workq-server` command line flags.

### cmdlog-path

Path to command log directory. The directory needs to be writable by the `workq-server` user. This option enables command logging with defaults.

**Example**

```
workq-server -cmdlog-path /path/to/workq/cmdlog
```

**Related Internals**:

* [Command Log Directory Structure](#log-directory-structure)
* [Log File Format](#log-file-format)

### cmdlog-sync

Disk sync policy to use for the command log. Defaults to `interval` mode with `cmdlog-sync-int` set to 1000 milliseconds.

Valid options:

* `interval` (Default) - Sync at a specified interval in milliseconds set by the `cmdlog-sync-int`, defaults to 1000 milliseconds.
* `os` - Sync deferred to the operating system.
* `always` - Sync after every command. This will be relatively slow.

**Example**

```
workq-server --cmdlog-path /path/to/workq/cmdlog --cmdlog-sync interval
```

### cmdlog-sync-int

Sync interval in milliseconds to use, defaults to 1000 milliseconds. This is to be used with `cmdlog-sync`  set to **interval**.

**Example**

```
workq-server -cmdlog-path /path/to/workq/cmdlog -cmdlog-sync interval --cmdlog-sync-interval 1000
```

### cmdlog-clean-int

Cleaning interval in milliseconds, defaults to 300000 milliseconds (5 minutes).

**Related Internals**:

* [Cleaning Cycle Strategy](#cleaning-cycle-strategy)

### cmdlog-seg-size

Minimum size of a single segment file, defaults to 64 MiB. After the minimum size has been reached, a new segment file is created for appending.

**Related Internals**:

* [Command Log Directory Structure](#log-directory-structure)

## Internals

### Log Directory Structure

Command logs are written to the path specified in `cmdlog-path` as sequentially named segment files, rolling into a new file as specified by `cmdlog-seg-size`.

Segment files are sequential 9 digit named files, left padded with 0s, with a "log" extension:

```
Format: {SEQUENCE}.log
Example: 000000001.log
```

### Log File Format

Log files start with a magic header and contain a sequence of variable size command records.

```text
|MAGIC_HEADER|RECORD|RECORD|RECORD|...
```

#### Magic header

Every log file starts with the following magic header (8 bytes) in hex: 77 71 77 71 00 00 00 01. The first four bytes translates to the utf8 string "wqwq" and the last 4 bytes is the version number "1" as an uint32 in big endian.

#### Record Format

A [job command is serialized](#command-serialization-format) stored inside a [Captain's Log Record](../int/captain/README.md) as the payload. All records are written in big endian format.

```text
Record
|TIME|SIZE|PAYLOAD|CRC|
```

* TIME (15 Bytes) - Time in UTC RFC 3399 with nanoseconds.
  * byte 0: version
  * bytes 1-8: seconds
  * bytes 9-12: nanoseconds
  * bytes 13-14: zone offset in minutes (reserved for future use, always 0 for UTC)
* SIZE (varint) - Length of payload
* PAYLOAD - Job command byte stream with length of SIZE.
* CRC (4 Bytes) - 32 bit hash computed on TIME,SIZE,PAYLOAD

#### Command Serialization Format

The following describes how each command's data is serialized within a [Record's payload](#record-format). Most commands share a similar format and are grouped below with differences noted.

##### add, schedule

Property | Data Type | Size | Notes
---- | --------- | ---- | -----
CMD_TYPE | uint | 1 |
JOB_ID | byte array | 16 | UUIDv4
JOB_NAME_LEN | varint | ~ |
JOB_NAME | string | JOB_NAME_LEN |
JOB_TTR | varint | ~ |
JOB_TTL | varint | ~ |
JOB_PRIORITY | varint | ~ |
JOB_MAX_ATTEMPTS | varint | ~ |
JOB_MAX_FAILS | varint | ~ |
JOB_PAYLOAD_LEN | varint | ~ |
JOB_PAYLOAD | byte array | JOB_PAYLOAD_LEN |
JOB_TIME | time | 15 | [Time Format](#time-format)
JOB_CREATED | time | 15 | [Time Format](#time-format)

* CMD_TYPE:
  * "add" is "1",
  * "schedule" is "2"

###### Time Format

Time in UTC RFC 3399 with nanoseconds.

* byte 0: version
* bytes 1-8: seconds
* bytes 9-12: nanoseconds
* bytes 13-14: zone offset in minutes (reserved for future use, always 0 for UTC)


##### complete, fail, delete

The following format maps for internal commands: "expire", "start-attempt", "timeout-attempt".

Successful lease commands translates to the internal "start-attempt" command which represents the single leased job in an idempotent fashion.

Property | Data Type | Size | Notes
---- | --------- | ---- | -----
CMD_TYPE | uint | 1 |
JOB_ID | byte array | 16 | UUIDv4

* CMD_TYPE:
  * "complete" command is "3",
  * "fail" command is "4"
  * "delete" is "5"
  * "expire" is 6
  * "start-attempt" is 7
  * "timeout-attempt" is 8

### Cleaning Cycle Strategy

Command log cleaning removes records that have expired. A record is considered expired if the related job has passed its TTL. Cleaning works by rewriting the segment file with only the relevant records. A Cleaner will only visit past read-only segmented files and not the most current one being appended to until it becomes read-only. This means that `cmdlog-seg-size` will always be the minimum size retained. Keeping segment-size reasonably sized will allow the cleaner to work efficiently.

The cleaner will start up in the background at the interval specified in `cmdlog-clean-int`. To prevent unnecessary constant rewrites, the Cleaner will average the expiration times across a single segment file and clean only when the average time is met. This allows the cleaning process to approximately halve a segment file at a time and skip cleanings altogether when it is not efficient.

Throughout cleaning, there is an exclusive lock on the read only segment files. This does not affect the latest active append file. Command log can continue to grow while cleaning is in operation.

#### Rewrite Process

The cleaner works on a single segment file at a time and creates a new temporary segment file using the same filename with a ".rw" extension. Any relevant records from the original segment file will be rewritten to this new file. At the end of the cleaning process, the rewrite file will be synced to disk. After a successful sync, the rewrite file is renamed to the original filename, atomically replacing the original segment file safely. Lastly, the directory where cmdlog resides is synced to ensure the rename is committed to disk.
