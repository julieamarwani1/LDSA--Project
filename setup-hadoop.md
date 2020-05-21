# Setup for both master and slaves
```
sudo apt install ssh
sudo apt install pdsh
nano .bashrc
export PDSH_RCMD_TYPE=ssh
ssh-keygen -t rsa -P ""
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys # Need to be copied to the slaves
sudo apt install openjdk-8-jdk
sudo wget -P ~ https://mirrors.sonic.net/apache/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz
tar xzf hadoop-3.2.1.tar.gz
mv hadoop-3.2.1 hadoop
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/' >> ~/hadoop/etc/hadoop/hadoop-env.sh
sudo mv hadoop /usr/local/hadoop
sudo echo 'PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/usr/local/hadoop/bin:/usr/local/hadoop/sbin"JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/jre"' >> /etc/environment
sudo adduser hadoopuser
sudo usermod -aG hadoopuser hadoopuser
sudo chown hadoopuser:root -R /usr/local/hadoop/
sudo chmod g+rwx -R /usr/local/hadoop/
sudo adduser hadoopuser sudo
```
## Master specific
```
sudo nano /etc/hostname ECHO hadoop-master
sudo reboot
su - hadoopuser
ssh-keygen -t rsa
ssh-copy-id hadoopuser@hadoop-master # Doesn't work always
ssh-copy-id hadoopuser@hadoop-slave1 # Manually copying the public
ssh-copy-id hadoopuser@hadoop-slave2 # Key to each instance 
                                     # authorized_keys works instead
sudo echo '<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>fs.defaultFS</name>
<value>hdfs://hadoop-master:9000</value>
</property>
</configuration>
' > /usr/local/hadoop/etc/hadoop/core-site.xml
sudo echo '<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>dfs.namenode.name.dir</name><value>/usr/local/hadoop/data/nameNode</value>
</property>
<property>
<name>dfs.datanode.data.dir</name><value>/usr/local/hadoop/data/dataNode</value>
</property>
<property>
<name>dfs.replication</name>
<value>2</value>
</property>
</configuration>' > /usr/local/hadoop/etc/hadoop/hdfs-site.xml  
sudo echo 'hadoop-slave1
hadoop-slave2' > /usr/local/hadoop/etc/hadoop/workers
scp /usr/local/hadoop/etc/hadoop/* hadoop-slave1:/usr/local/hadoop/etc/hadoop/
scp /usr/local/hadoop/etc/hadoop/* hadoop-slave2:/usr/local/hadoop/etc/hadoop/
source /etc/environment
hdfs namenode -format
start-dfs.sh # sometimes the PATH to hadoop/sbin/start-dfs.sh need to be used
```
### Configure YARN
```
export HADOOP_HOME="/usr/local/hadoop"
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
start-yarn.sh # sometimes the PATH to hadoop/sbin/start-dfs.sh need to be used

```

## Slave specific for X
```
sudo nano /etc/hostname ECHO hadoop-slaveX
sudo reboot
su - hadoopuser
```
### Configure YARN
```
# Unsure if it's okey to add another configuration 
sudo echo '<?xml version="1.0"?>
<configuration>
<property>
<name>yarn.resourcemanager.hostname</name>
<value>hadoop-master</value>
</property>
</configuration>' >> /usr/local/hadoop/etc/hadoop/yarn-site.xml    
```

