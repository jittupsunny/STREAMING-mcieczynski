#kafka-topics --zookeeper localhost:2181 --topic events --create --replication-factor=3 --partitions=6
export file="/tmp/botgen/logs/output1.log"
export topic="events"
export tasks="3"

echo "{\"name\": \"FileStreamSourceConnector\",\"config\": {\"connector.class\": \"org.apache.kafka.connect.file.FileStreamSourceConnector\",\"tasks.max\": \"$tasks\",\"file\": \"$file\",\"topic\": \"$topic\"}}" > FileStreamSourceConnector.json

curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d @FileStreamSourceConnector.json localhost:8091/connectors
