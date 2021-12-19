package dbs;
import java.util.Collections;
import java.io.Serializable;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
public class sparkzeug {
    public static void main(String[] args) {
    	System.setProperty("hadoop.home.dir", "c:\\winutil\\");
        SparkConf sparkConf = new SparkConf().setAppName("Stanni_Stats");
        sparkConf.setMaster("local[*]");
        System.setProperty("illegal-access", "permit");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> zwei = sparkSession.read().option("sep","|").option("inferSchema", true).json("C:\\Users\\User\\eclipse-workspace1\\keineahnungiwasspark\\src\\main\\java\\peter2.json");
        zwei.createOrReplaceTempView("zweitemp");
       // Dataset<Row> drei = sparkSession.sql("SELECT round(avg(user.followers_count),0) AS Standartprofilbild, round(Eigenes_Profilbild / round(avg(user.followers_count),0),0) AS Ratio, Eigenes_Profilbild, Stanni_Stats, Custom_Stats, round(Custom_Stats / Stanni_Stats,0) AS Stanni_Ratio  from (SELECT round(avg(user.followers_count),0) AS Eigenes_Profilbild from zweitemp WHERE user.default_profile_image = false), (SELECT round(avg(user.statuses_count),0) AS Custom_Stats from zweitemp WHERE user.default_profile_image = false), (SELECT round(avg(user.statuses_count),0) AS Stanni_Stats from zweitemp WHERE user.default_profile_image = true), zweitemp WHERE user.default_profile_image = true GROUP BY Eigenes_Profilbild,Stanni_Stats, Custom_Stats");
        zwei.show(2);
        
    }	
}
