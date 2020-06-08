# Zookeeper Quick Start

#### What is it?
```
ZooKeeper是Google的Chubby一个开源的实现，是一个开源的，分布式的应用程序协调服务  
是Hadoop和Hbase的重要组件  
它是一个为分布式应用提供一致性服务的软件  
提供的功能包括：配置维护、域名服务、分布式同步、组服务等  
ZooKeeper的目标就是封装好复杂易出错的关键服务，将简单易用的接口和性能高效、功能稳定的系统提供给用户。
```
#### 简介：
```
Zookeeper是有2n+1个节点（允许n个节点失效)组成的集群服务。  
在Zookeeper服务有两个角色，一个是leader，负责写服务和数据同步；剩下的是follower，提供读服务。
leader失效后会在follower中重新选举新的leader。(paxos算法)
每个follower都和leader有连接，接受leader的数据更新操作。（zab算法）
客户端可以连接到每个server，每个server的数据完全相同。
Server记录事务日志和快照到持久存储。
```
#### Zookeeper特点
```
最终一致性：为客户端展示同一个视图，这是zookeeper里面一个非常重要的功能
可靠性：如果消息被到一台服务器接受，那么它将被所有的服务器接受
实时性：Zookeeper不能保证两个客户端能同时得到刚更新的数据，如果需要最新数据，应该在读数据之前调用sync()接口
独立性 ：各个Client之间互不干预
原子性 ：更新只能成功或者失败，没有中间状态
顺序性 ：所有Server，同一消息发布顺序一致
```
#### Zookeeper中角色
```
领导者(Leader)：领导者负责进行投票的发起和决议，更新系统状态，处理写请求
跟随者(Follwer)：Follower用于接收客户端的读写请求并向客户端返回结果，在选主过程中参与投票
观察者（Observer）：观察者可以接收客户端的读写请求，并将写请求转发给Leader，但Observer节点不参与投票过程，只同步leader状态，Observer的目的是为了，扩展系统，提高读取速度。在3.3.0版本之后，引入Observer角色的原因：
Zookeeper需保证高可用和强一致性；
为了支持更多的客户端，需要增加更多Server；
Server增多，投票阶段延迟增大，影响性能；
权衡伸缩性和高吞吐率，引入Observer ；
Observer不参与投票；
Observers接受客户端的连接，并将写请求转发给leader节点；
加入更多Observer节点，提高伸缩性，同时不影响吞吐率。
客户端(Client)： 执行读写请求的发起方
```
#### Zookeeper数据模型
```
Zookeeper，内部是一个分层的文件系统目录树结构，每一个节点对应一个Znode。每个Znode维护着一个属性结构，它包含着版本号(dataVersion)，时间戳(ctime,mtime)等状态信息。ZooKeeper正是使用节点的这些特性来实现它的某些特定功能。每当Znode的数据改变时，他相应的版本号将会增加。每当客户端检索数据时，它将同时检索数据的版本号。并且如果一个客户端执行了某个节点的更新或删除操作，他也必须提供要被操作的数据版本号。如果所提供的数据版本号与实际不匹配，那么这个操作将会失败。
注：节点（znode）都可以存数据，可以有子节点 ；节点不支持重命名；数据大小不超过1MB（可配置）
```
#### Znode介绍
```
Znode是客户端访问ZooKeeper的主要实体，它包含以下几个特征：
（1）Watches
　　客户端可以在节点上设置watch(我们称之为监视器)。当节点状态发生改变时(数据的增、删、改)将会触发watch所对应的操作。当watch被触发时，ZooKeeper将会向客户端发送且仅发送一条通知，因为watch只能被触发一次。
（2）数据访问
　　ZooKeeper中的每个节点存储的数据要被原子性的操作。也就是说读操作将获取与节点相关的所有数据，写操作也将替换掉节点的所有数据。另外，每一个节点都拥有自己的ACL(访问控制列表)，这个列表规定了用户的权限，即限定了特定用户对目标节点可以执行的操作。
（3）节点类型
ZooKeeper中的节点有两种，分别为临时节点和永久节点。节点的类型在创建时即被确定，并且不能改变。
　　ZooKeeper的临时节点：该节点的生命周期依赖于创建它们的会话。一旦会话结束，临时节点将被自动删除，当然可以也可以手动删除。另外，需要注意是， ZooKeeper的临时节点不允许拥有子节点。
　　ZooKeeper的永久节点：该节点的生命周期不依赖于会话，并且只有在客户端显示执行删除操作的时候，他们才能被删除。
（4）顺序节点（唯一性的保证）
　　　当创建Znode的时候，用户可以请求在ZooKeeper的路径结尾添加一个递增的计数。这个计数对于此节点的父节点来说是唯一的，它的格式为”%10d”(10位数字，没有数值的数位用0补充，例如”0000000001”)。当计数值大于232-1时，计数器将溢出。

org.apache.zookeeper.CreateMode中定义了四种节点类型，分别对应：
PERSISTENT：永久节点
EPHEMERAL：临时节点
PERSISTENT_SEQUENTIAL：永久节点、序列化
EPHEMERAL_SEQUENTIAL：临时节点、序列化
```

### 安装
服务器环境：三台虚拟机，并且配置ssh互信：
centos1 192.168.137.122；
centos2 192.168.137.101；
centos3 192.168.137.102
- 1、下载、解压ZooKeeper：
```
wget http://mirrors.hust.edu.cn/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
解压到/usr/local/目录中：
tar -xvzf zookeeper-3.4.6.tar.gz -C /usr/local/
添加到环境变量：
ZOOKEEPER_HOME=/usr/local/zookeeper-3.4.6/
export PATH=$PATH:$ZOOKEEPER_HOME/bin:$ZOOKEEPER_HOME/conf
source /etc/profile
```
- 2、修改配置文件：
在zookeeper的conf/目录下，将zoo_sample.cfg重命名为zoo.cfg，然后：
将工作目录改为dataDir安装目录的data下（dataDir手工建立）：dataDir=/usr/local/zookeeper-3.4.6/dataDir
添加集群中的节点：
server.1=centos1:2888:3888
server.2=centos2:2888:3888
server.3=centos3:2888:3888
在dataDir目录下创建接myid文件：根据server.X的号码在相应的节点上的dataDir下建立myid文件，内容为X
- 3、以上在其中一台机子配置好了zookeeper，然后将zookeeper的目录scp到另外的两台机子上即可：
```
scp -r zookeeper-3.4.6 root@centos3:/usr/local/
```
在另外两台机子上修改myid文件内容；
在另外两台机子上配置环境变量；
- 4、启动zookeeper集群：
```
在三台机子上分别执行：zkServer.sh start
查看集群状态：zkServer.sh status
```
- 5、zookeeper配置文件说明：
tickTime=2000：ZK中的一个时间单元。ZK中所有时间都是以这个时间单元为基础，进行整数倍配置的。例如，session的最小超时时间是2*tickTime
initLimit=10：Follower在启动过程中，会从Leader同步所有最新数据，然后确定自己能够对外服务的起始状态。Leader允许F在initLimit时间内完成这个工作。通常情况下，我们不用太在意这个参数的设置。如果ZK集群的数据量确实很大了，F在启动的时候，从Leader上同步数据的时间也会相应变长，因此在这种情况下，有必要适当调大这个参数了。
syncLimit=5：在运行过程中，Leader负责与ZK集群中所有机器进行通信，例如通过一些心跳检测机制，来检测机器的存活状态。如果L发出心跳包在syncLimit之后，还没有从F那里收到响应，那么就认为这个F已经不在线了。注意：不要把这个参数设置得过大，否则可能会掩盖一些问题。
clientPort=2181：客户端连接server的端口，即对外服务端口，一般设置为2181。
server.1=centos1:2888:3888：2888端口号是zookeeper服务之间通信的端口，而3888是zookeeper与其他应用程序通信的端口。而zookeeper是在hosts中已映射了本机的ip。
- 命令行操作
1、连接到zookeeper服务：
```
# zkCli.sh -server centos3:2181 
连接到了centos3上的zookeeper服务，其中2181是端口号；
或者
# zkCli.sh   连接到本机的zookeeper服务；
```
2、连接到zk服务后，在命令行输入man或help命令，可以常看zk的命令：
```
[zk: localhost:2181(CONNECTED) 0] man
ZooKeeper -server host:port cmd args
	connect host:port
	get path [watch]
	ls path [watch]
	set path data [version]
	rmr path
	delquota [-n|-b] path
	quit 
	printwatches on|off
	create [-s] [-e] path data acl
	stat path [watch]
	close 
	ls2 path [watch]
	history 
	listquota path
	setAcl path acl
	getAcl path
	sync path
	redo cmdno
	addauth scheme auth
	delete path [version]
	setquota -n|-b val path
```
3、查看、创建、删除操作：
```
#ZooKeeper的结构，很像是目录结构，ls一下，看到了一个默认的节点zookeeper
[zk: localhost:2181(CONNECTED) 0] ls /
[zookeeper]
#创建一个新的节点，/node, 值是helloword
[zk: localhost:2181(CONNECTED) 6] create /node helloworld
Created /node
#再看一下，恩，多了一个我们新建的节点/node,和zookeeper都是在/根目录下的。
[zk: localhost:2181(CONNECTED) 7] ls /
[node, zookeeper]
#看看节点的值是啥？还真是我们设置的helloword,还显示了创建时间，修改时间，version,长度，children个数等
[zk: localhost:2181(CONNECTED) 8] get /node
helloworld
cZxid = 0x200000003
ctime = Sun Feb 21 16:23:53 CST 2016
mZxid = 0x200000003
mtime = Sun Feb 21 16:23:53 CST 2016
pZxid = 0x200000003
cversion = 0
dataVersion = 0
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 10
numChildren = 0
#修改值，看看，创时间没变，修改时间变了，长度变了，数据版本值变了
[zk: localhost:2181(CONNECTED) 9] set /node helloworld!
cZxid = 0x200000003
ctime = Sun Feb 21 16:23:53 CST 2016
mZxid = 0x200000004
mtime = Sun Feb 21 16:26:11 CST 2016
pZxid = 0x200000003
cversion = 0
dataVersion = 1
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 11
numChildren = 0
#创建子节点
[zk: localhost:2181(CONNECTED) 10] create /node/node1 node1
Created /node/node1
#查看子节点
[zk: localhost:2181(CONNECTED) 11] ls /node
[node1]
#查看父节点，其中numChildren 改变了
[zk: localhost:2181(CONNECTED) 12] get /node
helloworld!
cZxid = 0x200000003
ctime = Sun Feb 21 16:23:53 CST 2016
mZxid = 0x200000004
mtime = Sun Feb 21 16:26:11 CST 2016
pZxid = 0x200000005
cversion = 1
dataVersion = 1
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 11
numChildren = 1
#删除节点
[zk: localhost:2181(CONNECTED) 15] delete /test
```
- java 客户端
1、pom.xml
```
<dependency>
    <groupId>org.apache.zookeeper</groupId>
    <artifactId>zookeeper</artifactId>
    <version>3.4.6</version>
</dependency>
```
2、代码：
```java
private ZooKeeper zk = null; 
	
	/**
	 * 创建ZK连接
	 * @param connectString
	 * @param sessionTimeout
	 */
	 public void createConnection( String connectString, int sessionTimeout ) { 
	        this.releaseConnection(); 
	        try { 
	        	zk = new ZooKeeper(connectString, 
	        			sessionTimeout, new Watcher() { 
	                        // 监控所有被触发的事件
	                        public void process(WatchedEvent event) {
	                            // TODO Auto-generated method stub
	                            System.out.println("已经触发了" + event.getType() + "事件！"); 
	                        } 
	              });
	        } catch ( IOException e ) { 
	            System.out.println( "连接创建失败，发生 IOException" ); 
	            e.printStackTrace(); 
	        } 
	} 
	 
	/** 
     * 关闭ZK连接 
     */ 
    public void releaseConnection() { 
        if ( zk != null ) { 
            try { 
                this.zk.close(); 
            } catch ( InterruptedException e ) { 
                // ignore 
                e.printStackTrace(); 
            } 
        } 
    } 
    
    /** 
     *  创建节点 
     * @param path 节点path 
     * @param data 初始数据内容 
     * @return 
     */ 
    public boolean createPath( String path, String data ) { 
        try { 
        	String node = zk.create( path, data.getBytes(),Ids.OPEN_ACL_UNSAFE,  CreateMode.PERSISTENT );//临时、永久...节点
            System.out.println( "节点创建成功, Path: " 
                    +  node
                    + ", content: " + data ); 
        } catch ( KeeperException e ) { 
            System.out.println( "节点创建失败，发生KeeperException" ); 
            e.printStackTrace(); 
        } catch ( InterruptedException e ) { 
            System.out.println( "节点创建失败，发生 InterruptedException" ); 
            e.printStackTrace(); 
        } 
        return true; 
    }
    
    /** 
     * 读取指定节点数据内容 
     * @param path 节点path 
     * @return 
     */ 
    public String readData( String path ) { 
        try { 
        	String res = new String( zk.getData( path, false, null ) ); 
            System.out.println( "获取数据成功：" + res ); 
            return res;
        } catch ( KeeperException e ) { 
            System.out.println( "读取数据失败，发生KeeperException，path: " + path  ); 
            e.printStackTrace(); 
            return ""; 
        } catch ( InterruptedException e ) { 
            System.out.println( "读取数据失败，发生 InterruptedException，path: " + path  ); 
            e.printStackTrace(); 
            return ""; 
        } 
    }
    
    /** 
     * 更新指定节点数据内容 
     * @param path 节点path 
     * @param data  数据内容 
     * @return 
     */ 
    public boolean writeData( String path, String data ) { 
        try { 
        	
        	Stat stat = zk.setData( path, data.getBytes(), -1 );//-1表示匹配所有版本 
            System.out.println( "更新数据成功，path：" + path + ", stat: " + stat); 
        } catch ( KeeperException e ) { 
            System.out.println( "更新数据失败，发生KeeperException，path: " + path  ); 
            e.printStackTrace(); 
        } catch ( InterruptedException e ) { 
            System.out.println( "更新数据失败，发生 InterruptedException，path: " + path  ); 
            e.printStackTrace(); 
        } 
        return false; 
    }
    
    /** 
     * 删除指定节点 
     * @param path 节点path 
     */ 
    public void deleteNode( String path ) { 
        try { 
            zk.delete( path, -1 ); 
            System.out.println( "删除节点成功，path：" + path ); 
        } catch ( KeeperException e ) { 
            System.out.println( "删除节点失败，发生KeeperException，path: " + path  ); 
            e.printStackTrace(); 
        } catch ( InterruptedException e ) { 
            System.out.println( "删除节点失败，发生 InterruptedException，path: " + path  ); 
            e.printStackTrace(); 
        } 
    } 
    
    public List<String> getChildrens( String path ) { 
        try { 
            return zk.getChildren(path, true);
        } catch ( KeeperException e ) { 
            System.out.println( "删除节点失败，发生KeeperException，path: " + path  ); 
            e.printStackTrace(); 
            return null; 
        } catch ( InterruptedException e ) { 
            System.out.println( "删除节点失败，发生 InterruptedException，path: " + path  ); 
            e.printStackTrace(); 
            return null; 
        }
    }

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ZkTest test = new ZkTest();
		test.createConnection("192.168.137.122:2181", 3000);
		
		test.createPath("/node/node2", "node2");
		test.readData("/node");
		
		List<String> childrens = test.getChildrens("/node");
		for (String str :childrens) {
			System.out.println(str);
		}
		
		test.releaseConnection();
	}
#zookeeper
```

> @ WHAT - HOW - WHY  
> @ 不积跬步 - 无以至千里  
> @ 学必求其心得 - 业必贵其专精