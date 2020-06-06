#!/bin/bash
# bash create-slave.sh <PUBLIC_KEY> <MASTER_IP> <SLAVE_IP_1> ...

PUBLIC_KEY=$1

let NUMBER_OF_SLAVES=$#-2
let slave=0

sudo echo "$2 master" > /etc/hosts

for ip in "$@"
do
    if [ $slave -gt 1 ]
    then
    	let tmp_slave=$slave-1
    	sudo echo $ip "slave"$tmp_slave >> /etc/hosts
    	let slave+=1
    else
    	let slave+=1
    fi
done

sudo apt-get update
sudo apt-get upgrade -y

sudo service sshd restart
mkdir .ssh
echo $PUBLIC_KEY >> $HOME/.ssh/authorized_keys

sudo apt-get install openjdk-8-jdk -y
wget https://archive.apache.org/dist/hadoop/core/hadoop-2.7.3/hadoop-2.7.3.tar.gz
tar -xvf hadoop-2.7.3.tar.gz
echo 'if [ -f /etc/bashrc ]; then
        . /etc/bashrc
fi

export HADOOP_HOME=$HOME/hadoop-2.7.3
export HADOOP_CONF_DIR=$HOME/hadoop-2.7.3/etc/hadoop
export HADOOP_MAPRED_HOME=$HOME/hadoop-2.7.3
export HADOOP_COMMON_HOME=$HOME/hadoop-2.7.3
export HADOOP_HDFS_HOME=$HOME/hadoop-2.7.3
export YARN_HOME=$HOME/hadoop-2.7.3
export PATH=$PATH:$HOME/hadoop-2.7.3/bin

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin:$PATH' >> .bashrc
source .bashrc

echo "master" > /home/ubuntu/hadoop-2.7.3/etc/hadoop/masters
echo "master" > /home/ubuntu/hadoop-2.7.3/etc/hadoop/slaves

for tmp in $(seq 1 $NUMBER_OF_SLAVES)
do
    echo "slave"$tmp >> /home/ubuntu/hadoop-2.7.3/etc/hadoop/slaves
done


echo '# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-"/etc/hadoop"}
for f in $HADOOP_HOME/contrib/capacity-scheduler/*.jar; do
  if [ "$HADOOP_CLASSPATH" ]; then
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$f
  else
    export HADOOP_CLASSPATH=$f
  fi
done

export HADOOP_OPTS="$HADOOP_OPTS -Djava.net.preferIPv4Stack=true"
export HADOOP_NAMENODE_OPTS="-Dhadoop.security.logger=${HADOOP_SECURITY_LOGGER:-INFO,RFAS} -Dhdfs.audit.logger=${HDFS_AUDIT_LOGGER:-INFO,NullAppender} $HADOOP_NAMENODE_OPTS"
export HADOOP_DATANODE_OPTS="-Dhadoop.security.logger=ERROR,RFAS $HADOOP_DATANODE_OPTS"
export HADOOP_SECONDARYNAMENODE_OPTS="-Dhadoop.security.logger=${HADOOP_SECURITY_LOGGER:-INFO,RFAS} -Dhdfs.audit.logger=${HDFS_AUDIT_LOGGER:-INFO,NullAppender} $HADOOP_SECONDARYNAMENODE_OPTS"
export HADOOP_NFS3_OPTS="$HADOOP_NFS3_OPTS"
export HADOOP_PORTMAP_OPTS="-Xmx512m $HADOOP_PORTMAP_OPTS"
export HADOOP_CLIENT_OPTS="-Xmx512m $HADOOP_CLIENT_OPTS"
export HADOOP_SECURE_DN_USER=${HADOOP_SECURE_DN_USER}
export HADOOP_SECURE_DN_LOG_DIR=${HADOOP_LOG_DIR}/${HADOOP_HDFS_USER}
export HADOOP_PID_DIR=${HADOOP_PID_DIR}
export HADOOP_SECURE_DN_PID_DIR=${HADOOP_PID_DIR}
export HADOOP_IDENT_STRING=$USER' > /home/ubuntu/hadoop-2.7.3/etc/hadoop/hadoop-env.sh

echo '<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://master:9000</value>
</property>
</configuration>' > /home/ubuntu/hadoop-2.7.3/etc/hadoop/core-site.xml

echo '<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>dfs.replication</name>
<value>2</value>
</property>
<property>
<name>dfs.permissions</name>
<value>false</value>
</property>
<property>
<name>dfs.datanode.data.dir</name>
<value>/home/ubuntu/hadoop-2.7.3/datanode</value>
</property>
</configuration>' > /home/ubuntu/hadoop-2.7.3/etc/hadoop/hdfs-site.xml

echo '<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>mapreduce.framework.name</name>
<value>yarn</value>
</property>
</configuration>' > /home/ubuntu/hadoop-2.7.3/etc/hadoop/mapred-site.xml

echo '<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>yarn.nodemanager.aux-services</name>
<value>mapreduce_shuffle</value>
</property>
<property>
<name>yarn.nodemanager.auxservices.mapreduce.shuffle.class</name>
<value>org.apache.hadoop.mapred.ShuffleHandler</value>
</property>
</configuration>' > /home/ubuntu/hadoop-2.7.3/etc/hadoop/yarn-site.xml

wget https://downloads.lightbend.com/scala/2.13.2/scala-2.13.2.tgz
tar xvf scala-2.13.2.tgz
sudo mv scala-2.13.2 /usr/local/scala

echo "PATH=$PATH:/usr/local/scala/bin" >> .bashrc
source .bashrc

wget http://apache.mirrors.spacedump.net/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
tar xvf spark-2.4.5-bin-hadoop2.7.tgz
sudo mv spark-2.4.5-bin-hadoop2.7 /usr/local/spark

echo "PATH=$PATH:/usr/local/spark/bin" >> .bashrc
source .bashrc

cp /usr/local/spark/conf/spark-env.sh.template /usr/local/spark/conf/spark-env.sh
echo "SPARK_MASTER_HOST='$(hostname -I)'" >> /usr/local/spark/conf/spark-env.sh

# Check placement
echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> /usr/local/spark/conf/spark-env.sh

for tmp in $(seq 1 $NUMBER_OF_SLAVES)
do
    echo "slave"$tmp >> /usr/local/spark/conf/slaves
done

sudo chown ubuntu /home/ubuntu/hadoop-2.7.3
sudo chown ubuntu /usr/local/spark
sudo chown ubuntu /usr/local/scala

#sudo bash create-slave.sh "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDWaxlsq90EH+b4BBW46c/e2jRQyM6T4PGMX3jia9TkE8keSqgbDfKFxjqz7Q4bwFeXYsyMYu9bWNkS3byLTLF1YVbO/0SIoHGpt/qaPlEyfMmjDmMjcdNs6/pcFJMPD6/+8a508kH7LiCO9xVEYBJoJVk3u1NEZ4Bw5BE+XoJiYZvwHCqR0IaDB9j5S41dmdJy+MYIJ6EuT72jEi6w7fatrL7oK9alRrtLb5xZrV/ceJBlHPThqVOMPbzzah6A8+U+XWqmQ5fMw7sMnLpR0J/wO61lEY/jfykxEjhxUKJI92KlCCnZ6wBwhvLNBwKVqx+257P+Xwcp0RUKm7Ffingp root@master" 192.168.2.231 192.168.2.20 192.168.2.170 192.168.2.161 192.168.2.254
