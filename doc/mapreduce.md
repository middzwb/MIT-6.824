# MapReduce

-----

可扩展性；性能；容错；一致性；负载均衡

GFS2 Colossus

-----

倒排索引，

-----

并行计算，简单的编程模型，更好的利用物理资源。将容错，分布式，负载均衡，局部性优化，高可用，可靠性的细节隐藏在MapReduce之内

-----

* map阶段：读取输入，map -> k,v
* shuffle阶段：将map的输出根据partition将kv写入到相应的intermediate，reduce读取所有的intermediate并sort
* reduce阶段，读取intermediate中的kv并写入到tempfile，全部生成后rename到output。（避免中间状态）
