Step 1:
sudo apt-get update
sudo apt-get install default-jdk
wget https://downloads.apache.org/kafka/latest/kafka_2.13-2.8.0.tgz
tar -xzf kafka_2.13-2.8.0.tgz
cd kafka_2.13-2.8.0
mv kafka_2.13-2.8.0 kafka

Once installation is done make the following changes in zookeeper.properties
on each node:

vi kafka/config/zookeeper.properties

dataDir=/var/lib/zookeeper
# the port at which the clients will connect

clientPort=2181
# disable the per-ip limit on the number of connections since this is a non-production config
maxClientCnxns=0
tickTime=2000
initLimit=5
syncLimit=2

admin.enableServer=false
# replace your <internal ips>
server.1=10.128.0.5:2888:3888 
server.2=10.128.0.3:2888:3888
server.3=10.128.0.7:2888:3888


Step 2: make the following dirs and allow the root permission. Make it on every node
vi kafka/config/sever.properties
<username>--> your username

<id> uniquebroker id [1,2,3]

sudo mkdir -p /var/lib/zookeeper
sudo chown <username>:<username> /var/lib/zookeeper
echo <id> | sudo tee /var/lib/zookeeper/myid
ls -l /var/lib/zookeeper/myid


Step3:  Run zookeeper
~/kafka/bin/zookeeper-server-stop.sh # only if your are restarting

~/kafka/bin/zookeeper-server-start.sh -daemon ~/kafka/config/zookeeper.properties
tail -f ~/kafka/logs/zookeeper.out # To watch the logs


Step4: Change the server.properties on each node
broker.id= <id>

<internal_ips>
zookeeper.connect=10.128.0.5:2181,10.128.0.3:2181,10.128.0.7:2181

listeners=PLAINTEXT://<internal_ip of the node>:9092 
advertised.listeners=PLAINTEXT://<internal_ip of the node>:9092

Step5: Run kafka
~/kafka/bin/kafka-server-stop.sh

~/kafka/bin/kafka-server-start.sh -daemon ~/kafka/config/server.properties

# To view that brokers are connected and working
~/kafka/bin/zookeeper-shell.sh <internal_ip>:2181
ls /brokers/ids


Step 6:
Create topics and replicate on a single node:
~/kafka/bin/kafka-topics.sh --create --topic test-topic --bootstrap-server <internal_ip>:9092 --partitions 1 --replication-factor 1

#Now verify the topic replication on each node
~/kafka/bin/kafka-topics.sh --list --bootstrap-server <internal_ip>:9092



Step 7: 
Run  producer and consumer on separate nodes and verify the repication 
~/kafka/bin/kafka-topics.sh --list --bootstrap-server <internal_ip>:9092


Incase need to restart:

rm ~/kafka/logs/*
~/kafka/bin/zookeeper-server-stop.sh # only if your are restarting

~/kafka/bin/kafka-server-stop.sh
~/kafka/bin/zookeeper-server-start.sh -daemon ~/kafka/config/zookeeper.properties

~/kafka/bin/kafka-server-start.sh -daemon ~/kafka/config/server.properties
tail -f ~/kafka/logs/server.log

10.128.0.5:9092,10.128.0.3:9092,10.128.0.7:9092










