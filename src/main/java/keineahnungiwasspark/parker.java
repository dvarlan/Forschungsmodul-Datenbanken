package keineahnungiwasspark;
import java.util.*;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.arrow.vector.util.Text;
import org.apache.spark.SparkConf;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.io.*;
import edu.stanford.nlp.sentiment.*;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.coref.md.*;
import edu.stanford.nlp.tagger.common.*;
import java.io.*;
import org.apache.hadoop.io.*;

public class parker {
    public static void main(String[] args) {
    	System.setProperty("hadoop.home.dir", "C:\\hadoop-3.3.1");
       SparkConf sparkConf = new SparkConf().setAppName("Stanni_Stats");
       sparkConf.setMaster("local[*]");
        System.setProperty("illegal-access", "permit");
       SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
       Dataset<Row> zwei = sparkSession.read().option("inferSchema", true).json("C:\\Users\\User\\eclipse-workspace1\\keineahnungiwasspark\\src\\main\\java\\peter.json");
       // zwei.show();
     //  zwei.filter("col(text).contains('Messi')").show();
        zwei.createOrReplaceTempView("zweitemp");
     //   zwei = sparkSession.sql("Select text from zweitemp");
      //  zwei.show();
        Dataset<Row> filterer = sparkSession.read().option("inferSchema", true).option("sep",";").option("header", true).csv("C:\\Users\\User\\eclipse-workspace1\\keineahnungiwasspark\\src\\main\\java\\dbsp\\RSDB.csv");
        filterer.createOrReplaceTempView("filterertemp");
        filterer = sparkSession.sql("Select text from zweitemp, filterertemp WHERE zweitemp.text LIKE ('%' || ' ' || filterertemp.term || ' ' || '%') ");
        filterer.show(10000,false);
       // System.out.println(zwei.count());
      //  sparkSession.sql("Select term from filterertemp").show();
        
      // zwei.write().json("C:\\Users\\User\\eclipse-workspace1\\keineahnungiwasspark\\src\\main\\java\\TEXTE\\Peter\\Peter");
        //Dataset<Row> drei = sparkSession.sql("select (*) from zweitemp WHERE ((text LIKE '% ratio' OR text LIKE '% ratiod' OR text LIKE '% ratioed' OR text LIKE '% ratio %' OR text LIKE '%Ratio') AND (text NOT LIKE '%rational%' AND text NOT LIKE '%goal ratio%'))");
       // drei.createOrReplaceTempView("dreitemp");
        
        //Dataset<Row> funf = sparkSession.sql("select user, text from zweitemp WHERE (text LIKE '%cunt%' OR text LIKE '%prick%' OR text LIKE '%nonce%') GROUP BY user, text");
        //drei.show(1000,false);
        		//funf.show(10000,false);
       // Dataset<Row> vier = sparkSession.sql("SELECT(*) from dreitemp WHERE text != NULL");
      //  vier.show(1000000,false);
       // Dataset<Row> drei = sparkSession.sql("SELECT (*) From zweitemp WHERE text LIKE 'Messi%' ");
      // drei.show(3,false);
    	//String text2 = "Diese drecks kanaken in der s�dstadt haben mir meinen Merzedes stern geklaut";
    	//String text = "gawd damn chinks spreading dat corona virus US US US";
       //Properties props = new Properties();
      // props.setProperty("annotators", "tokenize, ssplit, pos, parse,sentiment");
     //  StanfordCoreNLP Pipeline = new StanfordCoreNLP(props);
     //  props.setProperty("file","C:\\Users\\User\\eclipse-workspace1\\keineahnungiwasspark\\src\\main\\java\\TEXTE\\Peter\\Peter\\Peterer\\smol\\smoller");
     //  props.setProperty("-pos.model","edu/stanford/nlp/models/pos-tagger/english-bidirectional-distsim.tagger");

      // StanfordCoreNLP Pipeline = new StanfordCoreNLP(props);
      //  props.setProperty("tokenize.options", "untokenizable = allDelete");
      //  props.setProperty("tokenize.options", "tokenizePerLine = true");
    //    props.setProperty("tokenize.options", "ud = true");
     //   props.setProperty("tokenize.options", "normalizeAmpersandEntity = true");
      //  props.setProperty("tokenize.options", "normalizeCurrency = true");
     //   props.setProperty("tokenize.options", "quotes = unicode");
     //   props.setProperty("tokenize.options", "splitAssimilations = false");
     //   props.setProperty("tokenize.options", "ellipses = unicode");
      //  props.setProperty("tokenize.options", "tokenizeNLs = false");
      //  props.setProperty("tokenize.options", "normalizeParentheses = true"); 
     //   StanfordCoreNLP Pipeline = new StanfordCoreNLP(props);
     //   try {
	//	Pipeline.run();
	//	} catch (IOException e) {
		//	// TODO Auto-generated catch block
	//	e.printStackTrace();
	//	}
       // ArrayList<File> c = new ArrayList<File>();
       // File d = new File("C:\\Users\\User\\eclipse-workspace1\\keineahnungiwasspark\\src\\main\\java\\2.txt");
       // File e = new File("C:\\Users\\User\\eclipse-workspace1\\keineahnungiwasspark\\src\\main\\java\\3.txt");
       // c.add(d);
       // c.add(e);

       // try {
		//	Pipeline.processFiles(c,true, null);
		//} catch (IOException e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
		//}
      //  try {
		//	Pipeline.run();
		//} catch (IOException e) {
			// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
        //CoreDocument document = Pipeline.processToCoreDocument();
    }	
}
