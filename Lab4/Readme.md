# Kafka Cluster Setup Guide

## Prerequisites
- Ubuntu Linux (tested on 20.04 LTS)
- Root or sudo access
- Java Development Kit (JDK)

## Overview
This guide walks you through setting up a 3-node Kafka cluster with ZooKeeper for distributed message streaming.

## Step 1: Installation

### Update System and Install Dependencies
```bash
sudo apt-get update
sudo apt-get install default-jdk wget
```

### Download and Extract Kafka
```bash
wget https://downloads.apache.org/kafka/latest/kafka_2.13-2.8.0.tgz
tar -xzf kafka_2.13-2.8.0.tgz
cd kafka_2.13-2.8.0
mv kafka_2.13-2.8.0 kafka
```

## Step 2: Configure ZooKeeper

Edit ZooKeeper configuration on each node:
```bash
vi kafka/config/zookeeper.properties
```

Configure the following settings:
```properties
# ZooKeeper Data Directory
dataDir=/var/lib/zookeeper

# Client Connection Port
clientPort=2181

# Connection Limits
maxClientCnxns=0
tickTime=2000
initLimit=5
syncLimit=2
admin.enableServer=false

# ZooKeeper Cluster Configuration
server.1=<node1_internalip>:2888:3888
server.2=<node2_internalip>:2888:3888
server.3=<node3_internalip>:2888:3888
```

## Step 3: Prepare ZooKeeper Data Directory

On each node, create and set permissions:
```bash
sudo mkdir -p /var/lib/zookeeper
sudo chown <username>:<username> /var/lib/zookeeper

# Create a unique ID for each node (1, 2, or 3)
echo <node_id> | sudo tee /var/lib/zookeeper/myid
ls -l /var/lib/zookeeper/myid
```

## Step 4: Configure Kafka Server

Edit server properties on each node:
```bash
vi kafka/config/server.properties
```

Update the following settings:
```properties
# Unique Broker ID for each node
broker.id=<node_id>

# ZooKeeper Connection String
zookeeper.connect=<node1_internalip>:2181,<node2_internalip>:2181,<node3_internalip>:2181

# Listeners Configuration
listeners=PLAINTEXT://<node_internalip>:9092
advertised.listeners=PLAINTEXT://<node_internalip>:9092
```

## Step 5: Start ZooKeeper

```bash
# Stop if already running (optional)
~/kafka/bin/zookeeper-server-stop.sh

# Start ZooKeeper
~/kafka/bin/zookeeper-server-start.sh -daemon ~/kafka/config/zookeeper.properties

# Watch logs
tail -f ~/kafka/logs/zookeeper.out
```

## Step 6: Start Kafka Brokers

```bash
# Stop if already running (optional)
~/kafka/bin/kafka-server-stop.sh

# Start Kafka
~/kafka/bin/kafka-server-start.sh -daemon ~/kafka/config/server.properties
```

## Step 7: Verify Cluster Setup

### Check Connected Brokers
```bash
~/kafka/bin/zookeeper-shell.sh <node1_internalip>:2181 ls /brokers/ids
```

### Create Test Topic
```bash
~/kafka/bin/kafka-topics.sh --create \
  --topic test-topic \
  --bootstrap-server <node1_internalip>:9092 \
  --partitions 1 \
  --replication-factor 3
```

### List Topics
```bash
~/kafka/bin/kafka-topics.sh --list --bootstrap-server <node1_internalip>:9092
```

## Troubleshooting

### Restart Cluster
```bash
# Clean logs
rm ~/kafka/logs/*

# Stop services
~/kafka/bin/zookeeper-server-stop.sh
~/kafka/bin/kafka-server-stop.sh

# Restart
~/kafka/bin/zookeeper-server-start.sh -daemon ~/kafka/config/zookeeper.properties
~/kafka/bin/kafka-server-start.sh -daemon ~/kafka/config/server.properties

# Check logs
tail -f ~/kafka/logs/server.log
```

## Notes
- Replace `<username>` with your Linux username
- Replace `<node_id>` with 1, 2, or 3 for each node
- Replace `<node_internalip>` with actual internal IP addresses
- This is a basic setup and should be further secured for production environments

## Recommended Next Steps
- Configure authentication
- Set up SSL/TLS
- Implement network security
- Configure log retention and compaction

## License
[Your License Here]

## Contributing
Contributions are welcome! Please submit a pull request or open an issue.