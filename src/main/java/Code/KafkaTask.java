package Code;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KafkaTask {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("yarn").getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.3.68.20:9092, 10.3.68.21:9092, 10.3.68.23:9092, 10.3.68.26:9092, 10.3.68.28:9092, 10.3.68.32:9092, 10.3.68.47:9092, 10.3.68.48:9092, 10.3.68.50:9092, 10.3.68.52:9092")
                .option("subscribe", "rt-queue_1")
                .load();
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        df.printSchema();
    }
}
