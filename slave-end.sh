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
