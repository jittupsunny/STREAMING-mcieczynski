kafka-topics --zookeeper localhost:2181 --topic events --delete
kafka-topics --zookeeper localhost:2181 --topic events --create --replication-factor=3 --partitions=6
