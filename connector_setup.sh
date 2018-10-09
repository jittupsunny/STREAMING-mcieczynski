export name="FileStreamSourceConnector2"
export file="/tmp/botgen/logs/output2.log"
export topic="events"
export tasks="3"
export batch="40000"

echo "{\"name\": \"$name\",\"config\": {\"connector.class\": \"org.apache.kafka.connect.file.FileStreamSourceConnector\",\"tasks.max\": \"$tasks\",\"file\": \"$file\",\"topic\": \"$topic\", \"batch.size\":\"$batch\"}}" > FileStreamSourceConnector.json

echo ""
curl -X DELETE http://localhost:8091/connectors/$name
echo ""
curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d @FileStreamSourceConnector.json localhost:8091/connectors
echo ""
