package Code;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;


import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
public class KafkaTask {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("yarn").getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.3.68.20:9092, 10.3.68.21:9092, 10.3.68.23:9092, 10.3.68.26:9092, 10.3.68.28:9092, 10.3.68.32:9092, 10.3.68.47:9092, 10.3.68.48:9092, 10.3.68.50:9092, 10.3.68.52:9092")
                .option("subscribe", "rt-queue_1")
                .load();
        Dataset<Row> res=df.selectExpr("CAST(value AS STRING)");

        res=res.select(split(col("value"),"\t").getItem(0).cast("long").cast("timestamp").as("time"),
                split(col("value"),"\t").getItem(6).cast("String").as("guid"),
                split(col("value"),"\t").getItem(4).cast("String").as("bannerId"));

        try {
            res.writeStream().format("console").outputMode("append").start().awaitTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

    }
}
