**Table of Contents**

- [Protocol](#protocol)
  - [Client commands](#client-commands)
    - [add](#add)
    - [schedule](#schedule)
    - [run](#run)
    - [result](#result)
  - [Worker commands](#worker-commands)
    - [lease](#lease)
    - [complete](#complete)
    - [fail](#fail)
    - [Administrative commands](#administrative-commands)
      - [delete](#delete)
    - [inspect](#inspect)
      - [inspect job by id](#inspect-job-by-id)
        - [inspect foreground or background jobs by name](#inspect-foreground-or-background-jobs-by-name)
      - [inspect scheduled jobs by name](#inspect-scheduled-jobs-by-name)
      - [inspect queue by name](#inspect-queue-by-name)
      - [inspect queues](#inspect-queues)
      - [inspect server](#inspect-server)
  - [Errors](#errors)
  - [Glossary](#glossary)
    - [Priority](#priority)
  	- [TTR - Time-to-run](#ttr---time-to-run)
  	- [TTL - Time-to-live](#ttl---time-to-live)
  	- [Lease](#lease)
  	- [Max-Attempts](#max-attempts)
  	- [Max-Fails](#max-fails)

# Protocol

Workq implements a simple, text based protocol. Clients interact with Workq over TCP socket in a request/response model with text commands.

There are 3 categories of commands in Workq: [Client](#client-commands), [Worker](#worker-commands), and [Administrative](#administrative-commands) commands. All commands are sent as ascii text lines terminated by `CR` followed by `LF` or `\r\n`.

Unstructured data such as job payloads or results are also terminated by `\r\n` and can contain any raw byte stream. All unstructured data are length prefixed from the preceding line allowing any abritatry data. Exact formats are specified in each command description.

All successful command responses will start with a `+` and all [errors](#errors) will start with a `-` as the first character. A client can use this pattern for success vs error logic branching.

Command flags specified in their individual commands descriptions are optional named arguments prefixed with `-` and must follow the `-key=value` format. For example, the priority flag is written as `-priority=10`.

## Client commands

### add

```text
add <id> <name> <ttr> <ttl> <payload-size> [-priority=<value>] [-max-attempts=<value>] [-max-fails=<value>]\r\n
<payload-bytes>\r\n
```

Enqueue a job by `name`.

* `id` - A [UUIDv4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) in canonical hex format. Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c4. A job id is client provided to act as a natural de-duplicator.
* `name` - Name of job in alphanumeric characters with the allowance of these special characters: `_`, `-`, `.`. Jobs are enqueued and grouped by name.
* `ttr` - Allowed time to run in milliseconds, max value is 86400000 (24 hours). TTR starts when a job is leased. When TTR ends, the job is requeued if allowed by the `max-attempts` flag.
* `ttl` - Max time to live in milliseconds for the job and its result regardless of state. Starts immediately after the job has been enqueued successfully. Max value is 2^64-1. TTL should include the time to process the job *and* retrieve its result.
* `payload-size` - Job payload size in bytes.
* `payload-bytes` - The byte stream to include as the job's payload. This exact payload without modification will be received by a worker. Max size is 1 MiB or 1,048,576 bytes.

**Optional Flags**

* `-priority` - Numeric priority from -2147483648 (lowest) to 2147483647 (highest), default is 0.
* `-max-attempts` - Max number of allowed attempts, default is "unlimited" up to the job's TTL. If a value is set, max is 255. An attempt is counted anytime a job is leased regardless of the outcome. This includes a job timing out due to TTR.
* `-max-fails` - Max number of explicit job failures. Explicit job failures are not from timeouts, but from explicit workers reporting results by the [`fail`](#fail) command. Use this when you want a job to be retried on "true" worker failures.

**Example**

```text
add 6ba7b810-9dad-11d1-80b4-00c04fd430c4 ping 1000 60000 4 -priority=10 -max-attempts=3 -max-fails=1\r\n
pong\r\n
```

#### Reply

**Success**

```text
+OK\r\n
```

[**Errors**](#errors)

### schedule

```text
schedule <id> <name> <ttr> <ttl> <time> <payload-size> [-priority=<value>] [-max-attempts=<value>] [-max-fails=<value>]\r\n
<payload-bytes>\r\n
```

Schedule a job at a future UTC time enqueued by name.

* `id` - A [UUIDv4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) in canonical hex format. Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c4. A job id is client provided to act as a natural de-duplicator.
* `name` - Name of job in alphanumeric characters with the allowance of these special characters: `_`, `-`, `.`. Jobs are enqueued and grouped by name.
* `ttr` - Allowed time to run in milliseconds, max value is 86400000 (24 hours). TTR starts when a job is leased. When TTR ends, the job is requeued if allowed by the `max-attempts` flag.
* `ttl` - Max time to live in milliseconds for the job and its result regardless of state. TTL is relative to the scheduled time so this means it **starts on** scheduled time. Max value is 2^64.
* `time` - UTC scheduled datetime to execute in RFC 3339 format without an explicit offset: Example: 2020-02-02T00:00:00Z
* `payload-size` - Job payload size in bytes.
* `payload-bytes` - The byte stream to include as the job's payload. This exact payload without modification will be received by a worker. Max size is 1 MiB or 1,048,576 bytes.


**Optional Flags**

* `-priority` - Numeric priority from 0 (default level, lowest) - 4294967295 (2^32) (highest).
* `-max-attempts` - Max number of allowed attempts, default is "unlimited" up to the job's TTL. If a value is set, max is 255. An attempt is counted anytime a job is leased regardless of the outcome. This includes a job timing out due to TTR.
* `-max-fails` - Max number of explicit job failures. Explicit job failures are not from timeouts, but from explicit workers reporting results by the [`fail`](#fail) command. Use this when you want a job to be retried on "true" worker failures.

**Example**

```text
schedule 6ba7b810-9dad-11d1-80b4-00c04fd430c7 ping 1000 2000 2016-06-14T06:46:46Z 4\r\n
pong\r\n
```

#### Reply

**Success**

```text
+OK\r\n
```

[**Errors**](#errors)

### run

```text
run <id> <name> <ttr> <wait-timeout> <payload-size> [-priority=<value>]\r\n
<payload-bytes>\r\n
```

Run a job, blocking until wait-timeout if no workers are available or until TTR, if a worker is processing.

* `id` - A [UUIDv4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) in canonical hex format. Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c4. A job id is client provided to act as a natural de-duplicator.
* `name` - Name of job in alphanumeric characters with the allowance of these special characters: `_`, `-`, `.`. Jobs are enqueued and grouped by name.
* `ttr` - Allowed time to run in milliseconds, max value is 86400000 (24 hours). TTR starts when a job is leased. When TTR ends, the client receives a `-TIMEOUT` error.
* `wait-timeout` - Milliseconds to wait for jobs to be picked up by a worker before timing out and returning control to the client.
* `payload-size` - Job payload size in bytes.
* `payload-bytes` - The byte stream to include as the job's payload. This exact payload without modification will be received by a worker. Max size is 1 MiB or 1,048,576 bytes.

**Optional Flags**

* `-priority` - Numeric priority from 0 (default level, lowest) - 4294967295 (2^32) (highest).

**Example**

```text
run 6ba7b810-9dad-11d1-80b4-00c04fd430c4 ping 1000 60000 4\r\n
ping\r\n
```

#### Reply

**Success Format**

```text
+OK <reply-count>\r\n
<id> <success> <result-size>\r\n
<result-bytes>\r\n
```

* `reply-count` - Will always be 1 for this command, signifying 1 result.
* `id` - Job [UUIDv4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) in canonical hex format. Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c4.
* `success` - `0` for failed, `1` for success.
* `result-size` - Result size in bytes.
* `result-bytes` - Result byte stream.

**Example**

```text
+OK 1\r\n
6ba7b810-9dad-11d1-80b4-00c04fd430c4 1 4\r\n
pong\r\n
```

[**Errors**](#errors)

### result

```text
result <id> <wait-timeout>\r\n
```

Get the result of a job by id, blocking until wait-timeout if requested jobs have not finished processing.

* `id` - Job [UUIDv4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) in canonical hex format. Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c4
* `wait-timeout` - Milliseconds to wait for the job's result before timing out and returning control to the client.

**Example**

```text
result 6ba7b810-9dad-11d1-80b4-00c04fd430c4 60000\r\n
```

#### Reply

**Success Format**

```text
+OK <reply-count>\r\n
<id> <success> <result-length>\r\n
<result-block>\r\n
```

* `reply-count` - Currently will always be 1 for this command, signifying 1 result.
* `id` - Job [UUIDv4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) in canonical hex format. Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c4
* `success` - `0` for failed, `1` for success.
* `result-size` - Result size in bytes.
* `result-bytes` - Result byte stream.

**Example**

```text
+OK 1\r\n
6ba7b810-9dad-11d1-80b4-00c04fd430c4 1 4\r\n
pong\r\n
```

[**Errors**](#errors)

## Worker commands

### lease

```text
lease <name>... <wait-timeout>\r\n
```

Lease a job by name. Multiple job names can be specified and they will be processed uniformly by random selection. This command is intended be executed within a loop with a reasonable wait-timeout.

* `name` - Name of job to lease in alphanumeric characters with the allowance of these special characters: `_`, `-`, `.`.
* `wait-timeout` - Milliseconds to wait for available jobs before timing out. It is recommended to keep this value reasonably low to avoid any "dead" connection issues. A good starting point is 60000.

**Special Considerations**

* There is no *hang forever* mode, however you can simulate this with a long <wait-timeout>. It is recommended to not set a long <wait-timeout> to reduce any unexpected TCP [dead peer](http://tldp.org/HOWTO/TCP-Keepalive-HOWTO/overview.html#checkdeadpeers) issues. In addition, a reasonable wait-timeout time allows the server to keep internals tidy.
* A worker lease workflow can cross connections. A worker can lease under one connection and finish processing in another connection. This relaxed constraints allows for ease of operation, but it allows for possible worker processing races. For example, a worker can finish a job with `complete` or `fail` **after** a TTR timeout **AND** even after another worker has picked up the job.

**Example**

```text
lease ping1 ping2 ping3 60000\r\n
```

#### Reply

**Success Format**

```
+OK <reply-count>\r\n
<id> <name> <payload-size>\r\n
<payload-bytes>\r\n
```

* `reply-count` - Currently will always be 1, signifying a single leased job.
* `id` - A [UUIDv4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) in canonical hex format. Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c4.
* `name` - Name of job in alphanumeric characters with the allowance of these special characters: `_`, `-`, `.`.
* `payload-size` - Job payload size in bytes.
* `payload-bytes` - The byte stream included as the job's payload.

**Example**

```text
+OK 1\r\n
6ba7b810-9dad-11d1-80b4-00c04fd430c4 ping 4\r\n
ping\r\n
```

[**Errors**](#errors)

### complete

```text
complete <id> <result-size>\r\n
<result-bytes>\r\n
```

Mark a job successfully completed with a result.

* `id` - Job [UUIDv4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) in canonical hex format. Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c4
* `result-size` - Result size in bytes.
* `result-bytes` - Result byte stream. Max size is 1 MiB or 1,048,576 bytes.

**Example**

```text
complete 6ba7b810-9dad-11d1-80b4-00c04fd430c4 4\r\n
pong\r\n
```

#### Reply

**Success**

```text
+OK\r\n
```

[**Errors**](#errors)

### fail

```text
fail <id> <result-size>\r\n
<result-bytes>\r\n
```

Mark a job failed with a result.

* `id` - Job [UUIDv4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) in canonical hex format. Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c4
* `result-size` - Result size in bytes.
* `result-bytes` - Result byte stream. Max size is 1 MiB or 1,048,576 bytes.

#### Reply

**Success**

```text
OK\r\n
```

[**Errors**](#errors)

### Administrative commands

#### delete

```text
delete <id>\r\n
```

Delete job by id.

* `id` - Job [UUIDv4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) in canonical hex format. Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c4.

#### Reply

**Success**

```text
+OK\r\n
```

[**Errors**](#errors)

### inspect

Inspect provides a series of subcommands to inspect specific objects.

The general format is:

```text
inspect <object>
```

#### inspect job by id

```text
inspect job <id>\r\n
```

Inspect a job by ID

* `id` - Job [UUIDv4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) in canonical hex format. Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c4.

**Example**

```text
inspect job 6ba7b810-9dad-11d1-80b4-00c04fd430c4\r\n
```

##### Reply

**Success Format**

```text
OK <reply-count>\r\n
<id> <key-count>\r\n
<key> <value>\r\n
// ... <key> <value> Repeats up to <key-count>
```

* `reply-count` - Will always be 1 for this command, signifying 1 job.
* `id` - UUIDv4 of the job.
* `key-count` - Signifies the number of key/value pairs returned for the job. It is necessary to read key-count as keys may be added in the future.

**Keys**

* `id` - A [UUIDv4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) in canonical hex format. Example: 6ba7b810-9dad-11d1-80b4-00c04fd430c4. A job id is client provided to act as a natural de-duplicator.
* `name` - Name of job in alphanumeric characters with the allowance of these special characters: `_`, `-`, `.`. Jobs are enqueued or grouped by name.
* `ttr` - Allowed time to run in milliseconds, max value is 86400000 (24 hours). TTR starts when a job is leased. When TTR ends, the job is requeued if allowed by the `max-attempts` flag.
* `ttl` - Max time to live in milliseconds for the job and its result regardless of state. Starts immediately after the job has been enqueued successfully. Max value is 2^64-1. TTL should include the time to process the job *and* retrieve its result.
* `payload-size` - Job payload size in bytes.
* `payload-bytes` - The byte stream to include as the job's payload. This exact payload without modification will be received by a worker.
* `max-attempts` - Max number of allowed attempts, default is "unlimited" up to the job's TTL. If a value is set, max is 255. An attempt is counted anytime a job is leased regardless of the outcome. This includes a job timing out due to TTR.
* `attempts` - Number of executed attempts.
* `max-fails` - Max number of explicit job failures. Explicit job failures are not from timeouts, but from explicit workers reporting results by the [`fail`](#fail) command. Use this when you want a job to beretried on "true" worker failures.
* `fails` - Number of current failures.
* `priority` - Numeric priority from 0 (default level, lowest) - 4294967295 (2^32) (highest)
* `state` - Current state of job as a numeric value. Mapping:
	* `0` - NEW, Job is brand new and untouched. Ready for processing.
	* `1` - COMPLETED, Job has been successfully completed. No further processing will occur.
	* `2` - FAILED, Job has failed, no further processing will occur.
	* `3` - PENDING, Job is in pending state to be processed. Most common when a job is retried for the second or successive attempts.
	* `4` - LEASED, Job has been leased by a worker.
* `created` - Job created UTC Time
* `time` - Job scheduled UTC time

**Example**

```text
+OK 1\r\n
6ba7b810-9dad-11d1-80b4-00c04fd430c4 12\r\n
name ping\r\n
ttr 1000\r\n
ttl 600000\r\n
payload-size 4\r\n
payload ping\r\n
max-attempts 0\r\n
attempts 0\r\n
max-fails 0\r\n
fails 0\r\n
priority 0\r\n
state 0\r\n
created 2016-08-22T01:50:51Z\r\n
```

[**Errors**](#errors)

#### inspect foreground or background jobs by name

```text
inspect jobs <name> <cursor-offset> <limit>\r\n
```

Inspect jobs by name without removing it from its queue. A cursor offset and limit is used to iterate the queue. Scheduled jobs that have not been awoken yet or have not met their scheduled time will not be returned. You can use [`inspect scheduled-jobs`] instead.

* `name` - Name of jobs in alphanumeric characters with the allowance of these special characters: `_`, `-`, `.`.
* `cursor-offset` - Cursor offset to start results from.
* `limit` - The number of results to return from cursor-offset.

**Example**

```text
inspect jobs ping 0 10\r\n
```

[**Errors**](#errors)

#### Reply

**Success Format**

```text
OK <reply-count>\r\n
<id> <key-count>\r\n
<key> <value>\r\n
// ... Repeats up to <key-count>
// ... Repeats up to <reply-count>
```

* `reply-count` - Signifies the number of jobs returned. This could be less than the `limit` arg if there are not enough jobs to return.
* `id` (UUIDv4) + `key-count` pair line signifies the beginning of the job's data as `<key> <value>\r\n` lines up to `key-count` and will repeat up to the `reply-count`. It is necessary to read key-count as keys may be added in the future.
	* `id` - UUIDv4 of the job.
	* `key-count` -Number of `<key> <value>\r\n` lines returned.

**Keys**

Same as [`inspect job <id>`].

**Example**

```text
+OK 2\r\n
6ba7b810-9dad-11d1-80b4-00c04fd430c4 12\r\n
name ping\r\n
ttr 1000\r\n
ttl 60000\r\n
payload-size 4\r\n
payload ping\r\n
max-attempts 0\r\n
attempts 0\r\n
max-fails 0\r\n
fails 0\r\n
priority 0\r\n
state 0\r\n
created 2016-08-22T01:50:51Z\r\n
6ba7b810-9dad-11d1-80b4-00c04fd430c6 12\r\n
name ping\r\n
ttr 1000\r\n
ttl 60000\r\n
payload-size 4\r\n
payload ping\r\n
max-attempts 0\r\n
attempts 0\r\n
max-fails 0\r\n
fails 0\r\n
priority 0\r\n
state 0\r\n
created 2016-08-22T02:00:17Z\r\n
```

#### inspect scheduled jobs by name

```text
inspect scheduled-jobs <name> <cursor-offset> <limit>\r\n
```

Inspect scheduled-jobs by name without removing it from its queue. A cursor offset and limit is used to iterate the queue.

* `name` - Name of jobs in alphanumeric characters with the allowance of these special characters: `_`, `-`, `.`.
* `cursor-offset` - Cursor offset to start results from.
* `limit` - The number of results to return from cursor-offset.

**Example**

```text
inspect scheduled-jobs ping 0 10\r\n
```

[**Errors**](#errors)

##### Reply

**Success Format**

```text
OK <reply-count>\r\n
<id> <key-count>\r\n
<key> <value>\r\n
// ... Repeats up to <key-count>
// ... Repeats up to <reply-count>
```

* `reply-count` - Signifies the number of jobs returned. This could be less than the `limit` param if there are not enough jobs to return.
* `id` + `key-count` - Signifies the beginning of the job's data as `<key> <value>\r\n` lines up to `key-count` and will repeat up to the `reply-count`.
	* `id` - UUIDv4 of the job.
	* `key-count` - Number of `<key> <value>\r\n` lines returned.

**Keys**

Same as [`inspect job <id>`]

**Example**

```text
+OK 2\r\n
6ba7b810-9dad-11d1-80b4-00c04fd430c9 13\r\n
name ping\r\n
ttr 1000\r\n
ttl 60000\r\n
payload-size 4\r\n
payload ping\r\n
max-attempts 0\r\n
attempts 0\r\n
max-fails 0\r\n
fails 0\r\n
priority 0\r\n
state 0\r\n
created 2016-08-22T02:16:30Z\r\n
time 2016-09-01T18:00:00Z\r\n
6ba7b810-9dad-11d1-80b4-00c04fd430c8 13\r\n
name ping\r\n
ttr 1000\r\n
ttl 60000\r\n
payload-size 4\r\n
payload ping\r\n
max-attempts 0\r\n
attempts 0\r\n
max-fails 0\r\n
fails 0\r\n
priority 0\r\n
state 0\r\n
created 2016-08-22T02:16:58Z\r\n
time 2016-09-01T19:00:00Z\r\n
```

[**Errors**](#errors)

#### inspect queue by name

```text
inspect queue <name>\r\n
```

Inspect a job queue by name.

* `name` - Name of the job's queue in alphanumeric characters with the allowance of these special characters: `_`, `-`, `.`.

**Example**

```text
inspect queue ping\r\n
```

##### Reply

**Success Format**

```text
OK <reply-count>\r\n
<name> <key-count>\r\n
<key> <value>\r\n
```

* `reply-count` - Always be 1 for this command, signifying a single job queue.
* `name` + `key-count` - Signifies the beginning of the queue's data as `<key> <value>\r\n` lines up to `key-count`.
	* `name` - Name of the queue.
	* `key-count` - Number of `<key> <value>\r\n` lines returned.

**Keys**

* `ready-len` - Number of enqueued jobs ready for processing, waiting for workers.
* `scheduled-len` - Number of scheduled jobs not ready for processing. Scheduled jobs are moved from here to `ready-len` when scheduled time is met.

**Example**

```text
OK 1\r\n
ping 4\r\n
ready-len 3\r\n
scheduled-len 0\r\n
```
[**Errors**](#errors)

#### inspect queues

```text
inspect queues <cursor-offset> <limit>\r\n
```

Inspect all available queues in alphabetical order.

* `cursor-offset` - Cursor offset to start results from.
* `limit` - The number of results to return from cursor-offset.

**Example**

```text
inspect queues 0 10\r\n
```

##### Reply

**Success Format**

```text
OK <reply-count>\r\n
<name> <key-count>\r\n
<key> <value>\r\n
// Repeats up to key-count.
// ...
// Repeats up to reply-count.
```

* `reply-count` - Signifies the number of queues returned.
* `name` + `key-count` - Signifies the beginning of the queue's data as `<key> <value>\r\n` lines up to `key-count` and will repeat up to the `reply-count`.
	* `name` - Name of the queue.
	* `key-count` - Number of `<key> <value>\r\n` lines returned.


**Keys**

Same as [`inspect queue <name>`].

**Example (2 queues)**

```text
+OK 2\r\n
ping1 2\r\n
ready-len 3\r\n
scheduled-len 0\r\n
ping2 2\r\n
ready-len 9\r\n
scheduled-len 0\r\n
```

[**Errors**](#errors)

#### inspect server

```text
inspect server\r\n
```

Inspect general server information.

##### Reply

**Sucess Format**

```text
OK <reply-count>\r\n
server <key-count>\r\n
<key> <value>\r\n
// Repeats up to <key-count>
```

* `reply-count` - Will always be 1 for this command, signifying one reply, the collective server information.
* `"server"` + `key-count` -  Signifies the beginning of the server's information as `<key> <value>\r\n` lines up to `key-count`.
	* `"server"` - Label for the information and represents the collective server information.
	* `key-count` - Number of `<key> <value>\r\n` lines returned.

**Keys**

* `active-clients` - Number of active & currently connected clients.
* `evicted-jobs` - Number of incomplete jobs expired by TTL.
* `started` - Time server started in UTC and RFC 3339 format.

**Example**

```text
OK 1\r\n
server 3\r\n
active-clients 32\r\n
evicted-jobs 0\r\n
started 2016-01-02T15:04:05Z\r\n
```

[**Errors**](#errors)

## Errors

All error responses start with `-` as the first character to signify an error type response.

* `-CLIENT-ERROR <error-description>` - Command is malformed (e.g, Missing required arguments, incorrect payload size)
* `-SERVER-ERROR <error-description>` - An error has occured while processing a command. This shouldn't happen during normal operation. Please file all server errors to our [Github Issues](https://github.com/iamduo/workq/issues)
* `-TIMEOUT` - Command failed to succeed within the `wait-timeout` supplied. Applicable to ["lease"](#lease), ["result"](#result), ["run"](#run) commands.
* `-NOT-FOUND` - Job ID specified not found.

## Glossary

### Priority

Priority is a signed 32-bit integer value from -2147483648 to 2147483647, 0 being the default. Controls the order of job execution within a single job queue. Higher priorities are executed first.

### TTR - Time-to-run

TTR stands for time-to-run and is the maximum time a job can run for after being [leased](#lease) before it is requeued. This value is in milliseconds.

### TTL - Time-to-live

Expiration time before job is deleted regardless of state. This value is in milliseconds.

### Lease

A worker receives work by leasing jobs by their name. A lease is bound by the job's [TTR](#ttr---time-to-run).

### Max-Attempts

Max Attempts is used in `add` and `schedule` commands to specify the number of times a job can be attempted before being marked as failed. An attempt is counted anytime a job is leased regardless of the outcome. This primarily is a job timing out due to [TTR](#ttr---time-to-run).

### Max-Fails

Max number of explicit job failures. Explicit job failures are not from timeouts, but from explicit workers reporting results by the [`fail`](#fail) command. Use this when you want a job to be retried on "true" worker failures and not timeouts from [TTR](#ttr---time-to-run).
