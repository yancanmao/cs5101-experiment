bin/kafka-topics.sh --alter --zookeeper localhost:2181 --topic wikipedia --partitions=10 --config message.timestamp.type=LogAppendTime
bin/kafka-topics.sh --alter --zookeeper localhost:2181 --topic wikinews --partitions=10 --config message.timestamp.type=LogAppendTime
bin/kafka-topics.sh --alter --zookeeper localhost:2181 --topic wiktionary --partitions=10 --config message.timestamp.type=LogAppendTime

bin/kafka-topics.sh --alter --zookeeper localhost:2181 --topic im_stream_1 --partitions=10
bin/kafka-topics.sh --alter --zookeeper localhost:2181 --topic im_stream_2 --partitions=10
bin/kafka-topics.sh --alter --zookeeper localhost:2181 --topic im_stream_3 --partitions=10