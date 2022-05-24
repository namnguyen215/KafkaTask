spark-submit --class KafkaTask\
    --deploy-mode client\
    --num-executors 5 \
    --executor-cores 2 \
    --executor-memory 2G \
    target/KafkaTask-1.0-SNAPSHOT.jar