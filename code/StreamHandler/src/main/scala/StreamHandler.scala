import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

object StreamHandler{
    def main(args: Array[String]){


        val spark = SparkSession
            .builder
            .appName("Stream Handler")
            .getOrCreate()
        
        import spark.implicits._

        val inputDF = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "Twitter_Stream")
            .load()
        
        val query = inputDF
            .writeStream
            .outputMode("update")
            .format("console")
            .start()
        
        query.awaitTermination()
    }
}