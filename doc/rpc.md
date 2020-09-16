# rpc

### task 1

see the source at https://golang.org/src/runtime/chan.go

### task 2

[Crawler exercise](https://tour.golang.org/concurrency/10)

### task 3

take a look at [Go's RPC package](https://pkg.go.dev/net/rpc)

sync.WaitGroup和ceph的gatherBuilder有点类似，目的都是一样的：等待多个任务完成。

Q: What are some important/useful Go-specific concurrency patterns to know?

A: Here's a slide deck on this topic, from a Go expert:

https://talks.golang.org/2012/concurrency.slide

Q: What are common debugging tools people use for Go?

A: fmt.Printf()

As far as I know there's not a great debugger for Go, though gdb can be
made to work:

https://golang.org/doc/gdb
