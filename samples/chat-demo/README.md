# Chat Demo Application

This is a small application for demonstrating the usage of MirBFT.
The comments in the code explain in detail how the library is instantiated and used.

This is a very minimal application that is not challenging the library in any way,
since at the time of writing this application, most of the library was composed of stubs.
As the library matures, the application can be extended to support state transfers, node restarts, etc.

## Running the application

4 nodes need to be started on the local machine, e.g. in 4 different terminal windows (from the root repository directory):

```bash
go run ./cmd/chat-demo 0
go run ./cmd/chat-demo 1
go run ./cmd/chat-demo 2
go run ./cmd/chat-demo 3
```

The application creates a `chat-demo-wal` wal directory where the nodes persist their state.
In case this directory is present after previous runs,
it can (and shoud) be deleted before starting the demo chat application.

When all 4 nodes are started, it may take a moment until they connect to each other.
Once all four nodes print a prompt the user to type their messages, the demo app can be used.
The processes read from standard input line by line
and all messages should be replicated across all four processes, using this library.
The process stops after reading EOF.
