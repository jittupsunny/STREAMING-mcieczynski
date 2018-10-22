kafka-topics --zookeeper localhost:2181 --topic events --create --replication-factor=3 --partitions=6
kafka-consumer-groups --bootstrap-server localhost:9092 --group bot-detection --reset-offsets --to-earliest --all-topics --execute
