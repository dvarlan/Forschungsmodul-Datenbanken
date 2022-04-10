package dbs;
import java.util.stream.*;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;
import edu.stanford.nlp.coref.data.CorefChain;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.ie.util.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.*;
import edu.stanford.nlp.trees.*;
import java.util.*;
import java.io.*;
import java.nio.file.*;
import java.nio.file.Files;
import java.time.Instant;
import edu.stanford.nlp.parser.*;
import com.kennycason.kumo.nlp.FrequencyFileLoader;
import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.PixelBoundaryBackground;
import com.kennycason.kumo.font.scale.LinearFontScalar;
import com.kennycason.kumo.nlp.FrequencyAnalyzer;
import com.kennycason.kumo.palette.ColorPalette;
import java.awt.*;

public class Sentimental {
	static int filecount = 0;
	public static Path split_Dir = Paths.get("/home/ubuntu/Raw_Data/");											//Input directory of the original large Json Files
	public static Path input_Dir = Paths.get("/home/ubuntu/Datein/");						//Input directory of the Split up Json Files or any empty directory if Files are split automatically
	public static Path result_Dir = Paths.get("/home/ubuntu/Ergebnis");					//Output directory of the result file containing only the text from negative sentiment tweets without metadata
	public static Path temp_Dir_1 = Paths.get("/home/ubuntu/zu_pruefen");					//A temporary directory
	public static Path filter_Dir = Paths.get("/home/ubuntu/FilterListen/merge.csv");	//The Input directory and file name of the list of words used as a filter
	public static Path temp_Dir_3 = Paths.get("/home/ubuntu/Output");							//Temporary output path for the sentiment analysis, containing still both positive and negative tweets
	public static Path negatives = Paths.get("/home/ubuntu/Negatives/");			//Finaler outputpfad in dem die Negativen Sentiment Dateien gespeichert werden
	public static String neg = "sentiment: Negative):";
	public static String veryneg = "sentiment: Very Negative):";
	public static Scanner scan;
	public static long[] tweet_counter = new long[47];
	public static long[] hate_tweets = new long[47];
	public static Path cloud = Paths.get("/home/ubuntu/cloud/");
   	public static Path cloud_picture = Paths.get("/home/ubuntu/picture/picture.png");
	
	 public static void main(String[] args) {
		if(args.length > 0 && args[0].equals("cloud"))
		   {
			   wordClouds(1);
			   return;
		   }
		 if(args.length > 0 && args[0].equals("hatecloud"))
		 {
			for(int i = 0;i < 100; i++) 
			{
				System.out.println("hatecloud inizialized");
			}
			cross_reference_as_Json();
			meta_delete_hatecloud();
			wordClouds(2);
			try {
				Files.move(Paths.get(cloud.toString()+ "result.png"),Paths.get(cloud.toString() + "hatecloud.png"));
			} catch (IOException e) {
				e.printStackTrace();
			}
			return;
		 }
		long unixstart = Instant.now().getEpochSecond();
		count_all_tweets();
		split();
		cross_reference();		
		meta_delete();		//dateien verbleiben danach in temp_Dir_1
		trim();			//dateien danach in input_Dir
		analyse_sentiment();	//dateien danach in temp_Dir_3
		move_negatives();	//dateien danach in negatives
		write_result();
	    	runtime(unixstart);
		tweet_percentage();
	 }

	public static void count_all_tweets() 
	{
	SparkConf sparkConf = new SparkConf().setAppName("counter");
	sparkConf.setMaster("local[*]");
	System.setProperty("illegal-access", "permit");
	SparkSession countSession = SparkSession.builder().config(sparkConf).getOrCreate();
	Stream <Path> count_stream = null;
	Dataset<Row> counting;
	try {
		count_stream = Files.walk(split_Dir);
	} catch (IOException e) {
		e.printStackTrace();
		System.out.println("counting");
		System.exit(8);
	}
	Object[] count_array_temp = count_stream.toArray();
	count_stream.close();
	Path[] count_array = Arrays.copyOf(count_array_temp, count_array_temp.length, Path[].class);
	for(Path tweets : count_array)
	{
		if(!tweets.equals(split_Dir))
		{
 			counting = countSession.read().option("inferSchema", true).json(tweets.toString());
			counting.createOrReplaceTempView("counting_view");
			counting = countSession.sql("Select timestamp_ms -1623436200000 AS Zeit from counting_view");
			counting.createOrReplaceTempView("timestamps");
			counting = countSession.sql("Select Zeit, ( case when Zeit < 0 THEN 45 When Zeit > 0 AND Zeit < 12600000 THEN 0 When Zeit > 64800000 AND Zeit < 75600000 THEN 1 When Zeit > 75600000 AND Zeit < 86400000 THEN 2 When Zeit > 86400000 AND Zeit < 97200000 THEN 3 When Zeit > 151200000 AND Zeit < 162000000 THEN 4 When Zeit > 162000000 AND Zeit < 172800000 THEN 5 When Zeit > 172800000 AND Zeit < 183600000 THEN 6 When Zeit > 237600000 AND Zeit < 248400000 THEN 7 When Zeit > 248400000 AND Zeit < 259200000 THEN 8 When Zeit > 259200000 AND Zeit < 270000000 THEN 9 When Zeit > 334800000 AND Zeit < 345600000 THEN 10 When Zeit > 345600000 AND Zeit < 356400000 THEN 11 When Zeit > 410400000 AND Zeit < 421200000 THEN 12 When Zeit > 421200000 AND Zeit < 432000000 THEN 13 When Zeit > 432000000 AND Zeit < 442800000 THEN 14 When Zeit > 496800000 AND Zeit < 507600000 THEN 15 When Zeit > 507600000 AND Zeit < 518400000 THEN 16 When Zeit > 518400000 AND Zeit < 529200000 THEN 17 When Zeit > 583200000 AND Zeit < 594000000 THEN 18 When Zeit > 594000000 AND Zeit < 604800000 THEN 19 When Zeit > 604800000 AND Zeit < 615600000 THEN 20 When Zeit > 669600000 AND Zeit < 680400000 THEN 21 When Zeit > 680400000 AND Zeit < 691200000 THEN 22 When Zeit > 691200000 AND Zeit < 702000000 THEN 23 When Zeit > 766800000 AND Zeit < 777600000 THEN 24 When Zeit > 853200000 AND Zeit < 864000000 THEN 25 When Zeit > 864000000 AND Zeit < 874800000 THEN 26 When Zeit > 950400000 AND Zeit < 961200000 THEN 27 When Zeit > 1026000000 AND Zeit < 1036800000 THEN 28 When Zeit > 1036800000 AND Zeit < 1047600000 THEN 29 When Zeit > 1285200000 AND Zeit < 1296000000 THEN 30 When Zeit > 1296000000 AND Zeit < 1308000000 THEN 31 When Zeit > 1371600000 AND Zeit < 1382400000 THEN 32 When Zeit > 1382400000 AND Zeit < 1393200000 THEN 33 When Zeit > 1458000000 AND Zeit < 1468800000 THEN 34 When Zeit > 1468800000 AND Zeit < 1479600000 THEN 35 When Zeit > 1544400000 AND Zeit < 1555200000 THEN 36 When Zeit > 1555200000 AND Zeit < 1566000000 THEN 37 When Zeit > 1803600000 AND Zeit < 1814400000 THEN 38 When Zeit > 1814400000 AND Zeit < 1825200000 THEN 39 When Zeit > 1890000000 AND Zeit < 1900800000 THEN 40 When Zeit > 1900800000 AND Zeit < 1911600000 THEN 41 When Zeit > 2160000000 AND Zeit < 2172000000 THEN 42 When Zeit > 2246400000 AND Zeit < 2258400000 THEN 43 When Zeit > 2592000000 AND Zeit < 2604000000 THEN 44 ELSE 46 END) AS Spiel from timestamps");
			counting.createOrReplaceTempView("games");
			counting = countSession.sql("Select Spiel, count(*) AS Anzahl from games Group BY Spiel ORDER BY Spiel ASC");
			java.util.List<Row> liste = counting.select("Anzahl").collectAsList();
			for(int i = 0;  i < tweet_counter.length && i < liste.size(); i++)
				{
					tweet_counter[i] = tweet_counter[i] + liste.get(i).getLong(0);
				}
		}
	}
	countSession.close();
	}
		
	public static void tweet_percentage()
	{
		float percent = -1;
		BufferedWriter frequency = null;
 	try {
		frequency = new BufferedWriter(new FileWriter( result_Dir.toString() + "//" + "frequency.txt",true));
	} catch (IOException e1) {
		e1.printStackTrace();
	}
		for(int i = 0; i < hate_tweets.length && i < tweet_counter.length; i++)
		{ 
			 	try {
					 System.out.println(tweet_counter[i]);
					 	if(tweet_counter[i] != 0)
						 {
							 percent = hate_tweets[i]/tweet_counter[i];
						 }
						 else
						 {
							 percent = -1;
						 }
						frequency.write("TEST");
						frequency.write(i);
						frequency.write(hate_tweets[i]);
						frequency.write("\t hate tweets:" + hate_tweets[i] + "\t all tweets:" + tweet_counter[i] + "\t Prozentual: "+ percent);
						frequency.write(match_matches(String.valueOf(i)) +"\t hate tweets:" + hate_tweets[i] + "\t all tweets:" + tweet_counter[i] + "\t Prozentual: "+ percent);
					} catch (IOException ioe) {
						System.out.println("ERROR FFS");
						ioe.printStackTrace();
						try
							{
								frequency.write("\t hate tweets:" + String.valueOf(hate_tweets[i]) + "\t all tweets:" + String.valueOf(tweet_counter[i]) + "\t Prozentual: "+ percent);
							} catch (IOException e11) {
								e11.printStackTrace();
							}
					}
		}
	}
	
	 public static void wordClouds(int version) {
		SparkConf sparkConf = new SparkConf().setAppName("Word_Cloud");
		sparkConf.set("spark.sql.optimizer.maxIterations", "300000");
	    sparkConf.setMaster("local[*]");
	    System.setProperty("illegal-access", "permit");
	    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
	    Stream<Path> paths = null;
	    Path cloud_Dir = null;
	    if(version == 1) {
		    cloud_Dir = split_Dir;
	    }
		 else
		 {
		    cloud_Dir = temp_Dir_3;
		 }
	    Dataset<Row> hashtags;
	    RelationalGroupedDataset grouped_table;
		try 
		{
			paths = Files.walk(cloud_Dir);
		} 
		catch (IOException e7) 
		{
			e7.printStackTrace();
			System.out.println("Debugcloud");
			System.exit(10);
		}
		Dataset<Row> hashtag_sums = null;
		boolean define_schema = true;
		Object[] temp = paths.toArray();
	    Path[] inputs = Arrays.copyOf(temp, temp.length, Path[].class);
	    Dataset<Row> juggle_Set = null;
	    Dataset<Row> leftAntiJoin; 
	    Integer i = 0;
	    for(Path input_path : inputs)
	    {	
	    	System.out.println("working");
    		if(!input_path.equals(cloud_Dir))
    		{
    			hashtags = sparkSession.read().option("inferSchema", true).json(input_path.toString()); //Pfad und name der einzulesenden Dateien hier "Datei(i).json"
    			hashtags = hashtags.select(hashtags.col("extended_tweet.entities.hashtags.text")).where(hashtags.col("extended_tweet.entities.hashtags").isNotNull().and(hashtags.col("extended_tweet.entities.hashtags.text").getItem(0).$greater("")));
    			hashtags = hashtags.select(functions.explode(hashtags.col("text")).as("einzelne_hashtags"));
    			grouped_table = hashtags.groupBy(hashtags.col("einzelne_hashtags"));
    			hashtags = grouped_table.count().withColumnRenamed("count","Hashtagcount");
    			
    			if(define_schema) // hashtag_sums beschreibt die Ergebnistabelle, beim ersten Anlaufen wird diese mit den Hashtags und deren Anzahl der Ersten Datei gefüllt
    			{
    				hashtag_sums = hashtags.select("einzelne_hashtags", "Hashtagcount").withColumnRenamed("einzelne_hashtags", "Hashtaggruppe").withColumnRenamed("Hashtagcount", "Häufigkeit");
    				define_schema = false;
    			} 
    			else 
    			{	

					juggle_Set = hashtag_sums.join(hashtags, hashtag_sums.col("Hashtaggruppe").equalTo(hashtags.col("einzelne_hashtags")), "left");
					juggle_Set = juggle_Set.where(juggle_Set.col("einzelne_hashtags").isNotNull()).withColumn("Häufigkeit", functions.col("Häufigkeit").plus(functions.col("Hashtagcount")));
					juggle_Set = juggle_Set.drop("einzelne_hashtags").drop("Hashtagcount");
					leftAntiJoin = hashtags.join(juggle_Set, hashtags.col("einzelne_hashtags").equalTo(juggle_Set.col("Hashtaggruppe")), "leftanti");
					juggle_Set = juggle_Set.union(leftAntiJoin);   				
    			}
    		}
	    }
	    juggle_Set = juggle_Set.select(juggle_Set.col("Häufigkeit"),juggle_Set.col("Hashtaggruppe"));
	    juggle_Set.show(100,false);
	    juggle_Set.write().option("sep", ":").csv(cloud.toString());
		Stream <Path> cloud_stream = null;
		try {
			cloud_stream = Files.walk(cloud);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Debugcloud");
			System.exit(8);
		}
		Object[] cloudarray = cloud_stream.toArray();
	    Path[] cloudpaths = Arrays.copyOf(cloudarray, cloudarray.length, Path[].class);
	    java.util.List<WordFrequency> frequencyList = null;
		FrequencyFileLoader loader = new FrequencyFileLoader();
		boolean once = true;
	    for(Path cloud_path : cloudpaths)
	    {	
    		if(!cloud_path.equals(cloud))
    			{
    				if(cloud_path.toString().contains("csv") && !cloud_path.toString().contains("crc"))
    				{
    					try {
    						if(once) {
        						frequencyList = loader.load(cloud_path.toFile());
        						once = false;
    						}
    						else {
    							frequencyList.addAll(loader.load(cloud_path.toFile()));
    						}
						} catch (IOException e) {
							e.printStackTrace();
						}

    				}
    			}
	    }
		final Dimension dimension = new Dimension(768, 624);
        final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
        wordCloud.setPadding(2);
        try {
			wordCloud.setBackground(new PixelBoundaryBackground(cloud_picture.toString()));
		} catch (IOException e) {
			e.printStackTrace();
		}
        wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
        wordCloud.setFontScalar(new LinearFontScalar(20, 50)); //Verändert die Größe der Schrift
        wordCloud.build(frequencyList);
        wordCloud.writeToFile(cloud.toString()+ "result.png"); //Output Dir.
	 }

	 public static void runtime(long unixstart) {

		long unixend = Instant.now().getEpochSecond(); // Endzeit
		int x = (int) (unixend - unixstart) / 60;
		System.out.println("Stunden: " + (x / 60) + " Minuten: " + (x % 60) + " Sekunden: " + ((unixend - unixstart) % 60));
	}

	 public static void cross_reference()
	 {
	    SparkConf sparkConf = new SparkConf().setAppName("Hate_Speech_Filter");
	    sparkConf.setMaster("local[*]");
	    System.setProperty("illegal-access", "permit");
	    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
	    Dataset<Row> filterdatei = sparkSession.read().option("inferSchema", true).option("sep",";").option("header", true).csv(filter_Dir.toString()); //die Optionen und der Pfad des Dictionaries mit dem gefiltert wird
	    filterdatei.createOrReplaceTempView("filter_liste");
	    Stream<Path> paths = null;
		try 
		{
			paths = Files.walk(input_Dir);
		} 
		catch (IOException e7) 
		{
			e7.printStackTrace();
			System.out.println("Debug2");
			System.exit(9);
		}
		Object[] temp = paths.toArray();
	    Path[] inputs = Arrays.copyOf(temp, temp.length, Path[].class);
	    int y = 1;
	    for(Path input_path : inputs)
	    {	
    		if(!input_path.equals(input_Dir))
    			{
    			Dataset<Row> zu_pruefen = sparkSession.read().option("inferSchema", true).json(input_path.toString()); //Pfad und name der einzulesenden Dateien hier "Datei(i).json"
    			zu_pruefen.createOrReplaceTempView("zu_pruefen_view");
    			filterdatei = sparkSession.sql("Select extended_tweet.full_text, timestamp_ms from zu_pruefen_view , filter_liste WHERE zu_pruefen_view.extended_tweet.full_text LIKE ('%' || ' ' || filter_liste.term || ' ' || '%') ");  //pruefen auf enthalten der Filterliste
    			filterdatei = filterdatei.dropDuplicates();
    			filterdatei.write().json(temp_Dir_1.toString() + "//" + Integer.toString(y));			//Output der gefilterten Datei, auf welche die sentimentanalyse ausgeführt wird
				y++;	
    			try {
					Files.delete(input_path);
				} catch (IOException e) {
					System.out.println("error deleting in cross referencing");
					e.printStackTrace();
				}
			}
	    }
	 }
	 
	 public static void meta_delete() 
	 {
		 int i = 1;
		 Stream<Path> a_stream = null;
		 try {
			 a_stream = Files.walk(temp_Dir_1);
		} 
		 catch (IOException e6) 
		{
			 e6.printStackTrace();
			 System.out.println("Debug3");
			 System.exit(9);
		}
	 	Object[] a_array_temp = a_stream.toArray();
 	    a_stream.close();
 	    Path[] a_array = Arrays.copyOf(a_array_temp, a_array_temp.length, Path[].class);
	    for(Path d : a_array)  			//alle metadateien werden gelöscht die zu pruefenden dateien werden umbenannt in einem inkrementierenden schema
	    {			
			if(!d.equals(temp_Dir_1))
			{
	    		if(d.toString().contains(".crc")) 
	    		{
	    			try 
	    			{
						Files.delete(d);
					} 
	    			catch (IOException e1) 
	    			{
						e1.printStackTrace();
						System.out.println("Debug4");
						System.exit(9);
					}
		    	}
		    	else if(d.toString().contains("_SUCCESS")) 
		    	{
		    		try 
		    		{
						Files.delete(d);
					} 
		    		catch (IOException e1) 
		    		{
						e1.printStackTrace();
						System.out.println("Debug5");
						System.exit(9);
					}
		    	}
		    	else if(d.toString().contains(".json"))
		    	{
		    		try 
		    		{
						filecount++;
						Files.move(d, Paths.get(temp_Dir_1.toString() + "//" + Integer.toString((int)Math.ceil(filecount/4)) + "_Datei_" + Integer.toString(i) +".json"),StandardCopyOption.REPLACE_EXISTING);
						i++;
		    		} 
		    		catch (IOException e1) 
		    		{
						e1.printStackTrace();
						System.out.println("Debug6");
						System.exit(9);
					}
		    	}
			}
	    }
		try {
			 a_stream = Files.walk(temp_Dir_1);
		} 
		 catch (IOException e6) 
		{
			 e6.printStackTrace();
			 System.out.println("Debug3");
			 System.exit(9);
		}
		a_array_temp = a_stream.toArray();
 	    a_stream.close();
 	    a_array = Arrays.copyOf(a_array_temp, a_array_temp.length, Path[].class);
	    for(Path d : a_array)
	    {
	    	if(d != null && !d.toString().contains(".json") && !d.equals(temp_Dir_1) )
	    	{
	    		try 
	    		{
					Files.delete(d);
				} catch (IOException e) 
	    		{
					e.printStackTrace();
					System.out.println("Debug_delete_empty_folders");
				}
	    	}
	    }
	 }
	 public static void split() 
	 {
		Stream <Path> split_stream = null;
		try {
			split_stream = Files.walk(split_Dir);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Debug14");
			System.exit(8);
		}
		Object[] split_array_temp = split_stream.toArray();
	 	split_stream.close();
	 	Path[] split_array = Arrays.copyOf(split_array_temp, split_array_temp.length, Path[].class);
	 	int x = 1;
	 	FileWriter splitter = null;
	 	int i = 0;
	 	Scanner tweetscanner = null;
	 	for(Path tweets : split_array)
	 	{
	 		if(!tweets.equals(split_Dir))
	 		{
				try {
					tweetscanner = new Scanner(tweets.toFile());
				} catch (FileNotFoundException e1) {
					e1.printStackTrace();
				}
		 		while(tweetscanner.hasNextLine()) 
		 		{
		 			try {
						splitter = new FileWriter(input_Dir.toString() + "//" + "Datei" + Integer.toString(x) + ".json");
						i = 0;
					} catch (IOException e) {
						e.printStackTrace();
						System.exit(x);
					}
	 				try 
	 				{
	 					while(tweetscanner.hasNextLine() && i < 10000)
	 					{
	 						splitter.write(tweetscanner.nextLine() + "\n");
	 						i++;
	 					}
	 					x++;
					} 
	 				catch (IOException e) 
	 				{
							e.printStackTrace();
							System.out.println("Debug15");
							System.exit(x);
					}
	 			}
	 			try 
	 			{
					splitter.close();
				} 
	 			catch (IOException e) 
	 			{
					e.printStackTrace();
					System.out.println("Debug16");
					System.exit(x);
				}
	 			try 
	 			{
					Files.delete(tweets);
				} 
	 			catch (IOException e) 
	 			{
					e.printStackTrace();
				}
	 		}
	 	}
	 }
	 
	 public static void analyse_sentiment() 
	 {
		 Properties props = new Properties();
		 props.setProperty("annotators", "tokenize, ssplit, pos, parse,sentiment");
		 props.setProperty("pos.model","edu/stanford/nlp/models/pos-tagger/english-left3words-distsim.tagger");
		 props.setProperty("tokenize.options", "tokenizeNLs=false,normalizeParentheses=true,ud=true,untokenizable=allDelete,tokenizePerLine=true,ellipses=unicode,normalizeAmpersandEntity=true,splitHyphenated=false,splitAssimilations=false,quotes=unicode,normalizeFractions=true,normalizeCurrency=true");
		 props.setProperty("ssplit.newlineIsSentenceBreak", "always");
		 props.setProperty("parse.model", "/home/ubuntu/github/dbsprojekt/edu/stanford/nlp/models/srparser/englishSR.ser.gz");		//schnellerer parser
		 props.setProperty("outputDirectory", temp_Dir_3.toString()); // Speicherort der Sentimentanalyse
		 props.setProperty("outputExtension", ".txt");
		 StanfordCoreNLP Pipeline = new StanfordCoreNLP(props);
		 Stream<Path> e_stream = null;
		 try 
		 {
			e_stream = Files.walk(temp_Dir_3);
		 } 
		 catch (IOException e5) 
		 {
			e5.printStackTrace();
			System.exit(9);
			System.out.println("Debug7");
		 }
		 Object[] e_array_temp = e_stream.toArray();
		 e_stream.close();
		 Path[] e_array = Arrays.copyOf(e_array_temp, e_array_temp.length, Path[].class);
		 for(Path f : e_array)			// Die sentimentanalyse wird ausgeführt 
		 {	
			 if(!f.equals(temp_Dir_3))
			 {		
				 props.setProperty("file", f.toString());
				 try 
				 {
					 
					 Pipeline.run();
				 } 
				 catch (IOException e1)
				 {
					 e1.printStackTrace();
					 System.out.println("Debug8");
					 System.exit(9);
				 }
				 try 
				 {
					 Files.delete(f.toAbsolutePath());
				 } 
			 	catch (IOException e1)
		 		 {
		 			 System.out.println("Debug9");
		 			 System.exit(9);
		 			 e1.printStackTrace();
				}
	    	}
		} 
	 }
	 
	 public static void move_negatives()
	 {
		 int i = 1; 
		 int z = 0;
		 Stream<Path> outputs_stream = null;
		 try 
		 {
			 outputs_stream = Files.walk(temp_Dir_3);
		 } 
		 catch (IOException e4) 
		 {
			 e4.printStackTrace();
		 }
		 Object[] outputs_array_temp = outputs_stream.toArray();
		 outputs_stream.close();
		 Path[] outputs_array = Arrays.copyOf(outputs_array_temp, outputs_array_temp.length, Path[].class);
		 for(Path g : outputs_array) 
		 {
			 boolean delete = true;
			 if(!g.equals(temp_Dir_3))
			 {
				 String temp_string;
				 try {
					 scan = new Scanner(g.toFile());
				 } 
				 catch (FileNotFoundException e)
				 {

					 e.printStackTrace();
				 }
				 while(scan.hasNextLine()) 
				 {
					 temp_string = scan.nextLine();
					 if(temp_string.contains(neg) || temp_string.contains(veryneg)) 
					 {
					 	scan.close();
					 	try 
					 	{
					 		delete = false;
					 		
					 		Files.move(g,  Paths.get(negatives.toString() + g.toString().substring(temp_Dir_3.toString().length())));
					 	} 
					 	catch (IOException e2) 
					 	{
					 		e2.printStackTrace();
					 		System.out.println("Debug10");
							System.exit(9);
					 	}
					 	break;
					 }
				 }
				 scan.close();
				 z++;
				 if(delete)
				 {
					 try 
					 {
						 Files.delete(g);
					 } 
					 catch (IOException e) 
					 {
						 e.printStackTrace();
					}
				 }
			 }	
		    	i++;
		}
	}
	 
	 public static void write_result() 
	 {
		 FileWriter result = null;
		 Stream<Path> negatives_stream = null;
		 try 
		 {
			 negatives_stream = Files.walk(negatives);
		 } 
		 catch (IOException e) 
		 {
			 e.printStackTrace();
			 System.out.println("Debug11");
			 System.exit(9);
		 }
		 Object[] negatives_array_temp = negatives_stream.toArray();
		 negatives_stream.close();
		 Path[] negatives_array = Arrays.copyOf(negatives_array_temp, negatives_array_temp.length, Path[].class);
		 for(Path h : negatives_array)
		 {

			 if(!h.equals(negatives))
			 {
				 try {
						result = new FileWriter( result_Dir.toString() + "//" + match_matches(h.toString().substring(negatives.toString().length()+1))+".json",true);
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					 
				 String temp_string;
				 try 
				 {
					scan = new Scanner(h.toFile());
				 } 
				 catch (FileNotFoundException e)
				 {
					e.printStackTrace();
				 }
				 while(scan.hasNextLine())
				 {
					 temp_string = scan.nextLine();
					 if(temp_string.contains(neg) || temp_string.contains(veryneg)) 
					 {
						 try 
						 {
							 temp_string = scan.nextLine().replace("{\"text\":\"", "").replace("”\"}", "").replace("\"}","").trim();
							 if(temp_string.length()>1) result.write( temp_string + "\n");
						 } 
						 catch (Exception e5) 
						 {
							 e5.printStackTrace();
							 System.out.println("Debug12");
							 System.exit(9);
						 }
					 }
		
				 }
				 scan.close();
				 try {
					Files.delete(h);
				 } 
				 catch (IOException e) 
				 {
					e.printStackTrace();
				 }
				 try {
					result.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
    		}
    		}	  
	 }
	 
	 public static void trim()
	 {
		 Stream<Path> trim_stream = null;
		 try {
			 trim_stream = Files.walk(temp_Dir_1);
		} 
		 catch (IOException e6) 
		{
			 e6.printStackTrace();
			 System.out.println("Debug3");
			 System.exit(9);
		}
		java.util.List<FileWriter> writers = new ArrayList<FileWriter>();
		
		for (int i = 0; i <= 46; i++) {
		    FileWriter w;
			try {
				w = new FileWriter(temp_Dir_3.toString() + "//" + Integer.toString(i));
			    writers.add(w);
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		
	 	Object[] trim_array_temp = trim_stream.toArray();
 	    trim_stream.close();
 	    int y = 0;
 	    String temp;
 	    String temp2;
 	    long zeit;
 	    Path[] trim_array = Arrays.copyOf(trim_array_temp, trim_array_temp.length, Path[].class);
	    for(Path x : trim_array)
	    {
			 if(!x.equals(temp_Dir_1))
			 {
				 try 
				 {
					scan = new Scanner(x.toFile());
				 } 
				 catch (FileNotFoundException e2)
				 {
					e2.printStackTrace();
				 }
				 if(scan.hasNextLine())
				 {
					 while(scan.hasNextLine())
					 {
						 temp = scan.nextLine(); 
						 try {
							if(temp.length() > 14) 
							{	
								temp2 = temp.substring(temp.length() - 15, temp.length()).replaceAll("\"}","");
								temp = temp.substring(14, temp.length() - 33);
								temp = temp.concat("\n");
								try {
								zeit =  Long.parseLong(temp2);
							    	} catch (NumberFormatException e) {
								continue;
							   	 }
								zeit -= 1623436200000L;
								if (zeit < 0L) {
									writers.get(45).write(temp); 
 									hate_tweets[45] = hate_tweets[45] + 1;
								}
								else if(zeit >= 0L && zeit < 12600000L)
								{
									writers.get(0).write(temp); 
 									hate_tweets[0] = hate_tweets[0] + 1;
								}
								else if(zeit >= 64800000L && zeit < 75600000L)
								{
									writers.get(1).write(temp); 
 									hate_tweets[1] = hate_tweets[1] + 1;
								}
								else if(zeit >= 75600000L && zeit < 86400000L)
								{
									writers.get(2).write(temp); 
 									hate_tweets[2] = hate_tweets[2] + 1;
								}
								else if(zeit >= 86400000L && zeit < 97200000L)
								{
									writers.get(3).write(temp); 
 									hate_tweets[3] = hate_tweets[3] + 1;
								}
								else if(zeit >= 151200000L && zeit < 162000000L)
								{
									writers.get(4).write(temp); 
 									hate_tweets[4] = hate_tweets[4] + 1;
								}
								else if(zeit >= 162000000L && zeit < 172800000L)
								{
									writers.get(5).write(temp); 
 									hate_tweets[5] = hate_tweets[5] + 1;
								}
								else if(zeit >= 172800000L && zeit < 183600000L)
								{
									writers.get(6).write(temp); 
 									hate_tweets[6] = hate_tweets[6] + 1;
								}
								else if(zeit >= 237600000L && zeit < 248400000L)
								{
									writers.get(7).write(temp); 
 									hate_tweets[7] = hate_tweets[7] + 1;
								}
								else if(zeit >= 248400000L && zeit < 259200000L)
								{
									writers.get(8).write(temp); 
 									hate_tweets[8] = hate_tweets[8] + 1;
								}
								else if(zeit >= 259200000L && zeit < 270000000L)
								{
									writers.get(9).write(temp); 
 									hate_tweets[9] = hate_tweets[9] + 1;
								}
								else if(zeit >= 334800000L && zeit < 345600000L)
								{
									writers.get(10).write(temp); 
 									hate_tweets[10] = hate_tweets[10] + 1;
								}
								else if(zeit >= 345600000L && zeit < 356400000L)
								{
									writers.get(11).write(temp); 
 									hate_tweets[11] = hate_tweets[11] + 1;
								}
								else if(zeit >= 410400000L && zeit < 421200000L)
								{
									writers.get(12).write(temp); 
 									hate_tweets[12] = hate_tweets[12] + 1;
								}
								else if(zeit >= 421200000L && zeit < 432000000L)
								{
									writers.get(13).write(temp); 
 									hate_tweets[13] = hate_tweets[13] + 1;
								}
								else if(zeit >= 432000000L && zeit < 442800000L)
								{
									writers.get(14).write(temp); 
 									hate_tweets[14] = hate_tweets[14] + 1;
								}
								else if(zeit >= 496800000L && zeit < 507600000L)
								{
									writers.get(15).write(temp); 
 									hate_tweets[15] = hate_tweets[15] + 1;
								}
								else if(zeit >= 507600000L && zeit < 518400000L)
								{
									writers.get(16).write(temp); 
 									hate_tweets[16] = hate_tweets[16] + 1;
								}
								else if(zeit >= 518400000L && zeit < 529200000L)
								{
									writers.get(17).write(temp); 
 									hate_tweets[17] = hate_tweets[17] + 1;
								}
								else if(zeit >= 583200000L && zeit < 594000000L)
								{
									writers.get(18).write(temp); 
 									hate_tweets[18] = hate_tweets[18] + 1;
								}
								else if(zeit >= 594000000L && zeit < 604800000L)
								{
									writers.get(19).write(temp); 
 									hate_tweets[19] = hate_tweets[19] + 1;
								}
								else if(zeit >= 604800000L && zeit < 615600000L)
								{
									writers.get(20).write(temp); 
 									hate_tweets[20] = hate_tweets[20] + 1;
								}
								else if(zeit >= 669600000L && zeit < 680400000L)
								{
									writers.get(21).write(temp); 
 									hate_tweets[21] = hate_tweets[21] + 1;
								}
								else if(zeit >= 680400000L && zeit < 691200000L)
								{
									writers.get(22).write(temp); 
 									hate_tweets[22] = hate_tweets[22] + 1;
								}
								else if(zeit >= 691200000L && zeit < 702000000L)
								{
									writers.get(23).write(temp); 
 									hate_tweets[23] = hate_tweets[23] + 1;
								}
								else if(zeit >= 766800000L && zeit < 777600000L)
								{
									writers.get(24).write(temp); 
 									hate_tweets[24] = hate_tweets[24] + 1;
								}
								else if(zeit >= 853200000L && zeit < 864000000L)
								{
									writers.get(25).write(temp); 
 									hate_tweets[25] = hate_tweets[25] + 1;
								}
								else if(zeit >= 864000000L && zeit < 874800000L)
								{
									writers.get(26).write(temp); 
 									hate_tweets[26] = hate_tweets[26] + 1;
								}
								else if(zeit >= 950400000L && zeit < 961200000L)
								{
									writers.get(27).write(temp); 
 									hate_tweets[27] = hate_tweets[27] + 1;
								}
								else if(zeit >= 1026000000L && zeit < 1036800000L)
								{
									writers.get(28).write(temp); 
 									hate_tweets[28] = hate_tweets[28] + 1;
								}
								else if(zeit >= 1036800000L && zeit < 1047600000L)
								{
									writers.get(29).write(temp); 
 									hate_tweets[29] = hate_tweets[29] + 1;
								}
								else if(zeit >= 1285200000L && zeit < 1296000000L)
								{
									writers.get(30).write(temp); 
 									hate_tweets[30] = hate_tweets[30] + 1;
								}
								else if(zeit >= 1296000000L && zeit < 1308000000L)
								{
									writers.get(31).write(temp); 
 									hate_tweets[31] = hate_tweets[31] + 1;
								}
								else if(zeit >= 1371600000L && zeit < 1382400000L)
								{
									writers.get(32).write(temp); 
 									hate_tweets[32] = hate_tweets[32] + 1;
								}
								else if(zeit >= 1382400000L && zeit < 1393200000L)
								{
									writers.get(33).write(temp); 
 									hate_tweets[33] = hate_tweets[33] + 1;
								}
								else if(zeit >= 1458000000L && zeit < 1468800000L)
								{
									writers.get(34).write(temp); 
 									hate_tweets[34] = hate_tweets[34] + 1;
								}
								else if(zeit >= 1468800000L && zeit < 1479600000L)
								{
									writers.get(35).write(temp); 
 									hate_tweets[35] = hate_tweets[35] + 1;
								}
								else if(zeit >= 1544400000L && zeit < 1555200000L)
								{
									writers.get(36).write(temp); 
 									hate_tweets[36] = hate_tweets[36] + 1;
								}
								else if(zeit >= 1555200000L && zeit < 1566000000L)
								{
									writers.get(37).write(temp); 
 									hate_tweets[37] = hate_tweets[37] + 1;
								}
								else if(zeit >= 1803600000L && zeit < 1814400000L)
								{
									writers.get(38).write(temp); 
 									hate_tweets[38] = hate_tweets[38] + 1;
								}
								else if(zeit >= 1814400000L && zeit < 1825200000L)
								{
									writers.get(39).write(temp); 
 									hate_tweets[39] = hate_tweets[39] + 1;
								}
								else if(zeit >= 1890000000L && zeit < 1900800000L)
								{
									writers.get(40).write(temp); 
 									hate_tweets[40] = hate_tweets[40] + 1;
								}
								else if(zeit >= 1900800000L && zeit < 1911600000L)
								{
									writers.get(41).write(temp); 
 									hate_tweets[41] = hate_tweets[41] + 1;
								}
								else if(zeit >= 2160000000L && zeit < 2172000000L)
								{
									writers.get(42).write(temp); 
 									hate_tweets[42] = hate_tweets[42] + 1;
								}
								else if(zeit >= 2246400000L && zeit < 2258400000L)
								{
									writers.get(43).write(temp); 
 									hate_tweets[43] = hate_tweets[43] + 1;
								}
								else if(zeit >= 2592000000L && zeit < 2604000000L)
								{
									writers.get(44).write(temp); 
 									hate_tweets[44] = hate_tweets[44] + 1;
								}
								else
								{
									writers.get(46).write(temp); 
 									hate_tweets[46] = hate_tweets[46] + 1;

								}
							} 
						 } catch (IOException e) 
						 {
							e.printStackTrace();
							}
					 }
					 y++;
					 scan.close();
					 x.toFile().delete();
			 }
	   	 }
	    }
	    
	   	for(FileWriter w : writers) {
	    	try {
				w.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		 }
 	}
	
	
	
	 public static String match_matches(String c) {
		 if(c.contains("txt"))
		    {
		 	c = c.replace(".txt", "");
		    }
		 int a = Integer.parseInt(c);
		 int i = 0;
		 if(a == i) {
			 return("TUR_ITA");
		 }
		 i++;
		 if(a == i) {
			 return("WAL_SUI");
		 }
		 i++;
		 if(a == i) {
			 return("DEN_FIN");
		 }
		 i++;
		 if(a == i) {
			 return("BEL_RUS");
		 }
		 i++;
		 if(a == i) {
			 return("ENG_CRO");
		 }
		 i++;
		 if(a == i) {
			 return("AUS_MAC");
		 }
		 i++;
		 if(a == i) {
			 return("NED_UKR");
		 }
		 i++;
		 if(a == i) {
			 return("SCO_CZE");
		 }
		 i++;
		 if(a == i) {
			 return("POL_SLOVAKIA");
		 }
		 i++;
		 if(a == i) {
			 return("ESP_SWE");
		 }
		 i++;
		 if(a == i) {
			 return("HUN_POR");
		 }
		 i++;
		 if(a == i) {
			 return("FRA_GER");
		 }
		 i++;
		 if(a == i) {
			 return("FIN_RUS");
		 }
		 i++;
		 if(a == i) {
			 return("TUR_WAL");
		 }
		 i++;
		 if(a == i) {
			 return("ITA_SUI");
		 }
		 i++;
		 if(a == i) {
			 return("UKR_MAC");
		 }
		 i++;
		 if(a == i) {
			 return("DEN_BEL");
		 }
		 i++;
		 if(a == i) {
			 return("NED_AUS");
		 }
		 i++;
		 if(a == i) {
			 return("SWE_SLOVAKIA");
		 }
		 i++;
		 if(a == i) {
			 return("CRO_CZE");
		 }
		 i++;
		 if(a == i) {
			 return("ENG_SCO");
		 }
		 i++;
		 if(a == i) {
			 return("HUN_FRA");
		 }
		 i++;
		 if(a == i) {
			 return("POR_GER");
		 }
		 i++;
		 if(a == i) {
			 return("ESP_POL");
		 }
		 i++;
		 if(a == i) {
			 return("SUI_TUR_AND_ITA_WAL");
		 }
		 i++;
		 if(a == i) {
			 return("MAC_NED_AND_UKR_AUS");
		 }
		 i++;
		 if(a == i) {
			 return("RUS_DEN_AND_FIN_BEL");
		 }
		 i++;
		 if(a == i) {
			 return("CRO_SCO_AND_CZE_ENG");
		 }
		 i++;
		 if(a == i) {
			 return("SWE_POL_AND_SLOV_ESP");
		 }
		 i++;
		 if(a == i) {
			 return("POR_FRA_AND_GER_HUN");
		 }
		 i++;
		 if(a == i) {
			 return("RO16_WAL_DEN");
		 }
		 i++;
		 if(a == i) {
			 return("RO16_ITA_AUS");
		 }
		 i++;
		 if(a == i) {
			 return("RO16_NED_CZE");
		 }
		 i++;
		 if(a == i) {
			 return("RO16_BEL_POR");
		 }
		 i++;
		 if(a == i) {
			 return("RO16_CRO_ESP");
		 }
		 i++;
		 if(a == i) {
			 return("RO16_FRA_SUI");
		 }
		 i++;
		 if(a == i) {
			 return("RO16_ENG_GER");
		 }
		 i++;
		 if(a == i) {
			 return("RO16_SWE_UKR");
		 }
		 i++;
		 if(a == i) {
			 return("RO8_SUI_ESP");
		 }
		 i++;
		 if(a == i) {
			 return("RO8_BEL_ITA");
		 }
		 i++;
		 if(a == i) {
			 return("RO8_CZE_DEN");
		 }
		 i++;
		 if(a == i) {
			 return("RO8_UKR_ENG");
		 }
		 i++;
		 if(a == i) {
			 return("SEMIS_ITA_ESP");
		 }
		 i++;
		 if(a == i) {
			 return("SEMIS_ENG_DEN");
		 }
		 i++;
		 if(a == i) {
			 return("FINAL_ENG_ITA");
		 }
		 i++;
		 if(a == i) {
			 return("PRE_CUP");
		 }
		 i++;
		 if(a == i) {
			 return("POST_CUP");
		 }
		 else {
			 return ("error");
		 }
	 }
	 public static void cross_reference_as_Json()
	 {
		    SparkConf sparkConf = new SparkConf().setAppName("Hate_Speech_Filter");
		    sparkConf.setMaster("local[*]");
		    System.setProperty("illegal-access", "permit");
		    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
		    Dataset<Row> filterdatei = sparkSession.read().option("inferSchema", true).option("sep",";").option("header", true).csv(filter_Dir.toString()); //die Optionen und der Pfad des Dictionaries mit dem gefiltert wird
		    filterdatei.createOrReplaceTempView("filter_liste");
		    Stream<Path> paths = null;
			try 
			{
				paths = Files.walk(split_Dir);
			} 
			catch (IOException e7) 
			{
				e7.printStackTrace();
				System.out.println("Debug2");
				System.exit(9);
			}
			Object[] temp = paths.toArray();
		    Path[] inputs = Arrays.copyOf(temp, temp.length, Path[].class);
		    int y = 1;
		    for(Path input_path : inputs)
		    {	
	    		if(!input_path.equals(split_Dir))
	    			{
	    			Dataset<Row> zu_pruefen = sparkSession.read().option("inferSchema", true).json(input_path.toString()); //Pfad und name der einzulesenden Dateien hier "Datei(i).json"
	    			zu_pruefen.createOrReplaceTempView("zu_pruefen_view");
	    			filterdatei = sparkSession.sql("Select * from zu_pruefen_view , filter_liste WHERE zu_pruefen_view.extended_tweet.full_text LIKE ('%' || ' ' || filter_liste.term || ' ' || '%') ");  //pruefen auf enthalten der Filterliste
	    			filterdatei.write().json(temp_Dir_3.toString() + "//" + Integer.toString(y));			//Output der gefilterten Datei, auf welche die sentimentanalyse ausgeführt wird
	    			System.out.println(y);
				y++;	
				}
		    }
		    sparkSession.close();
		 }
		 
	
		 public static void meta_delete_hatecloud() 
	 {
		 int i = 1;
		 Stream<Path> a_stream = null;
		 try {
			 a_stream = Files.walk(temp_Dir_3);
		} 
		 catch (IOException e6) 
		{
			 e6.printStackTrace();
			 System.out.println("Debug3");
			 System.exit(9);
		}
	 	Object[] a_array_temp = a_stream.toArray();
 	    a_stream.close();
 	    Path[] a_array = Arrays.copyOf(a_array_temp, a_array_temp.length, Path[].class);
	    for(Path d : a_array)  			//alle metadateien werden gelöscht die zu pruefenden dateien werden umbenannt in einem inkrementierenden schema
	    {			
			if(!d.equals(temp_Dir_3))
			{
	    		if(d.toString().contains(".crc") || d.toString().contains("_SUCCESS") ) 
	    		{
	    			try 
	    			{
						Files.delete(d);
					} 
	    			catch (IOException e1) 
	    			{
						e1.printStackTrace();
						System.out.println("Debug4");
						System.exit(9);
					}
		    	}
		    	else if(d.toString().contains(".json"))
		    	{
		    		try 
		    		{
						filecount++;
						Files.move(d, Paths.get(temp_Dir_3.toString() + "//" + Integer.toString((int)Math.ceil(filecount/4)) + "_Datei_" + Integer.toString(i) +".json"),StandardCopyOption.REPLACE_EXISTING);
						i++;
		    		} 
		    		catch (IOException e1) 
		    		{
						e1.printStackTrace();
						System.out.println("Debug6");
						System.exit(9);
					}
		    	}
			}
	    }
		try {
			 a_stream = Files.walk(temp_Dir_3);
		} 
		 catch (IOException e6) 
		{
			 e6.printStackTrace();
			 System.out.println("Debug3");
			 System.exit(9);
		}
		a_array_temp = a_stream.toArray();
 	    a_stream.close();
 	    a_array = Arrays.copyOf(a_array_temp, a_array_temp.length, Path[].class);
	    for(Path d : a_array)
	    {
	    	if(d != null && !d.toString().contains(".json") && !d.equals(temp_Dir_3) )
	    	{
	    		try 
	    		{
					Files.delete(d);
				} catch (IOException e) 
	    		{
					e.printStackTrace();
					System.out.println("Debug_delete_empty_folders");
				}
	    	}
	    }
	 }
	 
}

