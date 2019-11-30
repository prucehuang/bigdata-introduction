# 在Mac OS上部署Hadoop

### 1. 安装Homebrew和Cask  
打开Mac终端, 安装OS X 不可或缺的套件管理器homebrew和homebrew cask
```shell
$ ruby -e"$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"$ brew install caskroom/cask/brew-cask
```

### 2. 安装Java  
Hadoop是由Java编写, 所以需要预先安装Java 6或者更高的版本
```shell
$ brew update && brew upgrade brew-cask && brew cleanup && brew cask cleanup$ brew cask installjava

#测试是否安装成功
$ java -version
```

### 3. 配置SSH  
为了确保在远程管理Hadoop以及Hadoop节点用户共享时的安全性, Hadoop需要配置使用SSH协议  

首先在系统偏好设置->共享->打开远程登录服务->右侧选择允许所有用户访问
生成密钥对,执行如下命令
```shell
$ ssh-keygen -t rsa
```
执行这个命令后, 会在当前用户目录中的.ssh文件夹中生成id_rsa文件, 执行如下命令:
```shell
$ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```
使用下面命令测试是否能够不使用密码登录
```shell
$ ssh localhost# Last login: Thu Mar  517:30:072015
```

### 4. 安装Hadoop
```shell
$ brew install hadoop
```
Hadoop会被安装在/usr/local/Cellar/hadoop目录下
#### 4.1. 配置Hadoop
- 配置hadoop-env.sh  
```shell
vim /usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/hadoop-env.sh

export HADOOP_OPTS="$HADOOP_OPTS -Djava.net.preferIPv4Stack=true"
修改为:
export HADOOP_OPTS="$HADOOP_OPTS -Djava.net.preferIPv4Stack=true -Djava.security.krb5.realm= -Djava.security.krb5.kdc="
```

- 配置core-site.xml
```shell
vim /usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/core-site.xml

<configuration>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/usr/local/Cellar/hadoop/hdfs/tmp</value>
        <description>A base for other temporary directories.</description>
    </property>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

- 配置mapred-site.xml
```shell
vim /usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/mapred-site.xml

<configuration>
    <property>
        <name>mapred.job.tracker</name>
        <value>localhost:9010</value>
    </property>
</configuration>
```

- 配置hdfs-site.xml
```shell
vim /usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/hdfs-site.xml

<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1value>
    </property>
</configuration>
```

- 配置yarn-site.xml
```shell
vim /usr/local/Cellar/hadoop/2.6.0/libexec/etc/hadoop/yarn-site.xml

<property>
    <name>yarn.resourcemanager.address</name>
    <value>127.0.0.1:8032</value>
</property>
<property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>127.0.0.1:8030</value>
</property>
<property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>127.0.0.1:8031</value>
</property>
```
在运行后台程序前, 必须格式化新安装的HDFS, 并通过创建存储目录和初始化元数据创新空的文件系统, 执行下面命令:
```shell
$ hadoop namenode -format
```
#生成类似下面的字符串:
```shell
DEPRECATED: Use of this script to execute hdfs command is deprecated.Instead use the hdfs command for it.15/03/05 20:04:27 INFO namenode.NameNode:
/************************************************************
STARTUP_MSG: Starting NameNode
STARTUP_MSG:   host = Andrew-liudeMacBook-Pro.local/192.168.1.100
STARTUP_MSG:   args = [-format]STARTUP_MSG:   version = 2.6.0...
#此书省略大部分
STARTUP_MSG: java = 1.6.0_65
/***********************************************s*************
SHUTDOWN_MSG: Shutting down NameNode at Andrew-liudeMacBook-Pro.local/192.168.1.100
************************************************************/
```
#### 4.2. 启动后台程序

在/usr/local/Cellar/hadoop/2.6.0/sbin目录下, 执行如下命令
```shell
./start-dfs.sh  #启动HDFS$
./stop-dfs.sh  #停止HDFS
```
成功启动服务后, 可以直接在浏览器中输入http://localhost:50070/访问Hadoop页面

> @ 学必求其心得，业必贵其专精
> @ WHAT - HOW - WHY
> @ 不积跬步 - 无以至千里