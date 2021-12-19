package keineahnungiwasspark;
import java.util.Collections;
import java.io.Serializable;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.functions;

public class augabe3 {
	public static void main(String[] args) {
	System.setProperty("hadoop.home.dir", "c:\\winutil\\");
    SparkConf sparkConf = new SparkConf().setAppName("Test");
    sparkConf.setMaster("local[*]");
    System.setProperty("illegal-access", "permit");
    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    Dataset<Row> zwei = sparkSession.read().option("sep","|").option("inferSchema", true).csv("C:\\Users\\User\\eclipse-workspace1\\keineahnungiwasspark\\src\\main\\java\\part.tbl");
    zwei.filter("(LENGTH(_c1) - LENGTH(replace(_c1, ' ', ''))) = 4").show();
    }		
}
