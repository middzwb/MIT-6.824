## MapReduce Test

第一次跑test时*Starting map parallelism test*和*Starting crash test*失败了。

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

改完之后又跑了几次，`map parallelism`也都过了。

### 总结

总的来说，写一个玩具版的mr还是比较简单的，稍微注意下每个阶段结束的处理。

## Raft Test

### 2A

实现选举，比较简单，在家里用手机写，然后跑过了
