# 程序设计划重点

## 程序基础知识

1. 逻辑结构
2. 存储结构
3. 时间复杂度／空间复杂度／存储密度（空间利用率）

## 线性表

插入
删除
查找
长度
清空
是否为空

## 顺序表
链表
增
删
查
反转
合并

栈：
push 
pop
size
clear
是否为空

队列：
enter
delete
gethead
size
clear
是否为空
顺序队列
链栈链式

栈和队列的应用：
1. 表达式括号是否配对
2. 两个栈模拟一个队列
3. 两个队列模拟一个栈

稀疏矩阵及存储

树
二叉树基本特性
二叉树的遍历（递归与非递归）
二叉树的深度，遍历路径，广度遍历
平衡二叉树，
二叉搜索树
红黑树
哈夫曼树与哈夫曼编码

图
图的基本特性
有向图／无向图
度／出度／入度
图的存储（邻接矩阵，邻接表）
图的遍历
最短路径算法
拓扑图的遍历。

查找：
折半查找

排序：
冒泡
选择
快速排序
堆排
二路归并

基础
存储层次
局部性原理，代码实现
cpu主频
常用时间常量
CPU时间 & sys时间
top命令解析

多线程
多线程安全
互斥 & 同步
同步队列的实现
java锁类型
— synchronized
— reentranlock
— contidion
— semphore机制
- cas & volatile

— 容器机制
— hashmap & hashtable
— stringbuffer & stringbuilder
— hashset
— treemap & treeset
— priority queue
— stack & vector


- 内存模型
- 内存模型生成（编译阶段）
— java内存模型
— jvm进程分析（ps jstat，jstack，jmap,pmap）
pmap查看内存，大jar，大so。
1. cpu负载高
2. jmap -heap 查看堆内存使用情况
ps -mp 188379 -o THREAD,tid,time,rss,size


问题：
heap space oom
stackoverflow
java程序，指定了 -xmx，但是res却非常高。

Java Heap 溢出（Java heap spacess）
虚拟机栈和本地方法栈溢出（StackOverflowError）
运行时永久区溢出（PermGen space），如常量池等
方法区溢出 （PermGen space）
线程过多（unable to creat new native thread）
GC效率低下 (GC overhead limit exceeded)
直接内存内存溢出 (Direct buffer memory)
数组过大 （Requested array size exceeds VM limit）>=Integer.MAX_VALUE-1时


实时系统设计：
通信协议的选取
限流机制
服务注册与发现
负载均衡

