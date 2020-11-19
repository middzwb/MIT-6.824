## MapReduce Test

* Starting map parallelism test输出`saw 0 workers rather than 2`

后面又跑了两次，`map parallelism`成功了一次，另一次`saw 1 workers rather than 2`。而crash test一直失败。我的crash实现是master和worker之间保持心跳，超时后重新将task分配给其他的worker，没有重新拉起worker，看了脚本和代码后，确实是随机退出某个worker进程，实现没有问题。单独调试crash，发现master忘记开接收心跳的线程了!!!(ｷ｀ﾟДﾟ´)!!

调试reduce crash的时候，发现是自己写的判断reduce阶段是否结束的逻辑有问题。因为当样本不足的情况下，确实会存在某个reduce的intermediate输入为空，不能以此条件作为reduce阶段结束的依据。我的玩具版本以`id == -1`作为结束的判断。

```
❯ sh ./test-mr.sh
*** Starting wc test.
2020/09/20 20:22:33 rpc.Register: method "Done" has 1 input parameters; needs exactly three
--- wc test: PASS
*** Starting indexer test.
2020/09/20 20:22:36 rpc.Register: method "Done" has 1 input parameters; needs exactly three
--- indexer test: PASS
*** Starting map parallelism test.
2020/09/20 20:22:39 rpc.Register: method "Done" has 1 input parameters; needs exactly three
--- map parallelism test: PASS
*** Starting reduce parallelism test.
2020/09/20 20:22:46 rpc.Register: method "Done" has 1 input parameters; needs exactly three
--- reduce parallelism test: PASS
*** Starting crash test.
2020/09/20 20:22:55 rpc.Register: method "Done" has 1 input parameters; needs exactly three
2020/09/20 20:23:45 reduce finished
rm: cannot remove '/var/tmp/824-mr-0': No such file or directory
--- crash test: PASS
*** PASSED ALL TESTS
```

总的来说，写一个玩具版的mr还是比较简单的，稍微注意下每个阶段结束的处理。

## Raft Test

### 2A

实现选举，比较简单，在家里用手机写，然后跑过了

### 2B

apply to client的时候测试一直失败，最后打印输出后发现要求是**每个节点**都需要把commit的日志回复给client，所以commitIndex是有用的呀.

### 2C

改的头皮发麻。

-----

首先是2B的遗漏。

* 使用matchIndex更新commitIndex
* 优化entry不一致时的reject回复，发送当前term的日志来代替单条发送
* heartbeat消息不附带日志
* 发送entry时，如果已经有更大的日志，则丢弃当前消息，因为更大的日志会附带当前日志

TODO: 心跳消息只在业务负载低的时候发送，负载高的时候，AppendEntries本身已起到心跳作用(看zookeeper论文有感)

#### 2C

2C改的头皮发麻,改了两天，还是不清楚原因。从日志上看，本应该100ms发送的的心跳隔了500ms甚至更大才发送；还有AppendEntries的发送时间也很奇怪，不连续,发送给两个不同sever的entry中间相差很大，能有几百ms，明明是在一个循环里的。

-----

改2C期间，因为觉得代码逻辑没问题，而且错误的case是因为频繁在选举，所以怀疑可能是超时时间的设置和更新有bug。但是怎么改都不行。最后看了test case的实现，结合日志打印的时间戳确认，某些原因导致了程序获取不到资源，无法执行代码。我当时还很奇怪，单跑case是没问题的，但是一跑全部的2C test，各种fail情况都出现了，应该早点想到是这个原因的。

感觉可能是资源占用过多，用`top`看了一下，cpu和内存都没异常。突然灵机一动，在send AppendEntries和RequestVote的地方加了当前raft是否kill的判断，如果不判断的话，可能出现当前raft实例退出，但是发rpc的协程还没完成，结果无限重试。改完后，终于跑过了，看了一下日志，跑一个2c的日志，退出了2000+的协程，怪不得。

### 3B

chan没做关闭的处理，导致死锁
