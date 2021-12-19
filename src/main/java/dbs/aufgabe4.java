package dbs;
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

public class aufgabe4 {
public static void main(String[] args) {
	System.setProperty("hadoop.home.dir", "c:\\winutil\\");
    SparkConf sparkConf = new SparkConf().setAppName("Test");
    sparkConf.setMaster("local[*]");
    System.setProperty("illegal-access", "permit");
    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    Dataset<Row> zwei = sparkSession.read().option("sep","|").option("inferSchema", true).csv("C:\\Users\\User\\eclipse-workspace1\\keineahnungiwasspark\\src\\main\\java\\partsupp.tbl");
    zwei.createOrReplaceTempView("zweitemp");
    
    Dataset<Row> drei = sparkSession.sql("SELECT _c1, COUNT(*) AS Anzahl FROM zweitemp GROUP BY _c1 ORDER BY _c1 asc");
    drei.show(1002);
    }		
}
