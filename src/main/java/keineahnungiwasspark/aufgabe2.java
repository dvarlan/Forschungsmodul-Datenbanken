package keineahnungiwasspark;
import java.util.Collections;
import java.io.Serializable;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
public class aufgabe2 {
    public static void main(String[] args) {
    	System.setProperty("hadoop.home.dir", "c:\\winutil\\");
        SparkConf sparkConf = new SparkConf().setAppName("Test");
        sparkConf.setMaster("local[*]");
        System.setProperty("illegal-access", "permit");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> zwei = sparkSession.read().option("sep","|").option("inferSchema", true).csv("C:\\Users\\User\\eclipse-workspace1\\keineahnungiwasspark\\src\\main\\java\\part.tbl");
        zwei.groupBy("_c2","_c3").sum("_c7").sort("_c2").show((int) zwei.count());
	    }	
}

