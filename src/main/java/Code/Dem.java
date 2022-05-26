package Code;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

public class Dem {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("yarn").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.3.68.20:9092, 10.3.68.21:9092, 10.3.68.23:9092, 10.3.68.26:9092, 10.3.68.28:9092, 10.3.68.32:9092, 10.3.68.47:9092, 10.3.68.48:9092, 10.3.68.50:9092, 10.3.68.52:9092")
                .option("subscribe", "rt-queue_1")
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load();
        Dataset<Row> value=df.selectExpr("CAST(value AS STRING)");
        value=value.select(split(col("value"),"\t").getItem(0).cast("long").cast("timestamp").as("time"),
                split(col("value"),"\t").getItem(6).cast("String").as("guid"),
                split(col("value"),"\t").getItem(4).cast("String").as("bannerId"));
//        value=value.filter("time >= '2022-05-24 06:00:00' ").filter("time <= '2022-05-26 06:00:00'");
        value.select(min("time")).show();
        value.select(max("time")).show();


    }
}
