kafka-topics --zookeeper localhost:2181 --topic events --create --replication-factor=3 --partitions=6
kafka-consumer-groups --bootstrap-server localhost:9092 --group bot-detection --reset-offsets --to-earliest --all-topics --execute

export name="FileStreamSourceConnector3"
export file="/tmp/botgen/logs/output3.log"
export topic="events"
export tasks="3"
export batch="40000"

echo "{\"name\": \"$name\",\"config\": {\"connector.class\": \"org.apache.kafka.connect.file.FileStreamSourceConnector\",\"tasks.max\": \"$tasks\",\"file\": \"$file\",\"topic\": \"$topic\", \"batch.size\":\"$batch\"}}" > FileStreamSourceConnector.json

curl -X DELETE localhost:8091/connectors/$name
kafka-consumer-groups --bootstrap-server localhost:9092 --group bots --reset-offsets --to-earliest --all-topics --execute
curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d @FileStreamSourceConnector.json localhost:8091/connectors
