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
	public static long tweet_counter = 0;
	public static long hate_tweets = 0;
	public static Path cloud = Paths.get("/home/ubuntu/cloud/");
   	public static Path cloud_picture = Paths.get("/home/ubuntu/picture/picture.png");
	
	 public static void main(String[] args) {
		if(args.length() > 0 && args[0].equals("cloud"))
		   {
			   wordClouds();
			   return;
		   }
		long unixstart = Instant.now().getEpochSecond();
		split();
		cross_reference();
		meta_delete();
		trim();
		analyse_sentiment();
		move_negatives();
		write_result();
	    	runtime(unixstart);
	    	System.out.println(((int) ((tweet_counter) / hate_tweets!=0?hate_tweets:1)) + "%");
		System.out.println("Tweets: " + tweet_counter);
		System.out.println("Detected Hate Tweets: " + hate_tweets);
	 }

	 public static void wordClouds() {
		SparkConf sparkConf = new SparkConf().setAppName("Word_Cloud");
	    sparkConf.setMaster("local[*]");
	    System.setProperty("illegal-access", "permit");
	    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
	    Stream<Path> paths = null;
	    Dataset<Row> hashtags;
		try 
		{
			paths = Files.walk(split_Dir);
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
	    Integer i = 0;
	    for(Path input_path : inputs)
	    {	
    		if(!input_path.equals(input_Dir))
    		{
    			hashtags = sparkSession.read().option("inferSchema", true).json(input_path.toString()); //Pfad und name der einzulesenden Dateien hier "Datei(i).json"
    			hashtags.createOrReplaceTempView("hashtagview");
    			hashtags = hashtags.sparkSession().sql("Select extended_tweet.entities.hashtags.text FROM hashtagview WHERE extended_tweet.entities.hashtags.text IS NOT NULL AND LENGTH(extended_tweet.entities.hashtags.text[0]) > 0");
    			hashtags = hashtags.select(functions.explode(hashtags.col("text")).as("einzelne_hashtags"));
    			hashtags.createOrReplaceTempView("hashtagviewneu");
    			hashtags = hashtags.sparkSession().sql("SELECT einzelne_hashtags, COUNT(*) AS Hashtagcount FROM hashtagviewneu GROUP BY einzelne_hashtags");
    			hashtags.createOrReplaceTempView("merge");
    			if(define_schema) 
    			{
    				hashtag_sums = sparkSession.sql("SELECT * FROM merge");
    				define_schema = false;
    			} 
    			else 
    			{	
    				hashtag_sums.createOrReplaceTempView("sums" + i.toString());
    				hashtag_sums = hashtag_sums.sparkSession().sql("SELECT v1.einzelne_hashtags, v1.Hashtagcount AS Hashtagcount,v2.Hashtagcount AS to_add FROM " + "sums" + i.toString() +  " v1 JOIN merge v2 ON v1.einzelne_hashtags = v2.einzelne_Hashtags");
    				hashtag_sums.createOrReplaceTempView("sums1" + i.toString());
    				hashtag_sums = hashtag_sums.sparkSession().sql("SELECT einzelne_hashtags, (Hashtagcount + to_add) AS Hashtagcount from sums1" + i.toString());
    				hashtag_sums = hashtag_sums.drop("to_add");
    				hashtag_sums.createOrReplaceTempView("sums11" + i.toString());
    				hashtag_sums = hashtag_sums.sparkSession().sql("SELECT * FROM sums11" + i.toString() +  " UNION (SELECT * FROM  merge LEFT ANTI JOIN sums1" +i.toString() + " )");
    				i++;
    			}
				hashtag_sums.show(100,false);

    		}
	    }
	    hashtag_sums.createOrReplaceTempView("swap");
	    hashtag_sums = hashtag_sums.sparkSession().sql("SELECT Hashtagcount, einzelne_hashtags FROM swap ");
	    hashtag_sums.write().option("sep", ":").csv(cloud.toString());
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
	    for(Path cloud_path : cloudpaths)
	    {	
    		if(!cloud_path.equals(cloud))
    			{
    				if(cloud_path.toString().contains("csv") && !cloud_path.toString().contains("crc"))
    				{
    					FrequencyFileLoader loader = new FrequencyFileLoader();
    					try {
    						frequencyList = loader.load(cloud_path.toFile());
						} catch (IOException e) {
							e.printStackTrace();
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
    			        break;
    				}
    			}
	 }
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
    			filterdatei = sparkSession.sql("Select extended_tweet.full_text from zu_pruefen_view , filter_liste WHERE zu_pruefen_view.extended_tweet.full_text LIKE ('%' || ' ' || filter_liste.term || ' ' || '%') ");  //pruefen auf enthalten der Filterliste
    			filterdatei = filterdatei.dropDuplicates();
			filterdatei.write().json(temp_Dir_1.toString() + "//" + Integer.toString(y));			//Output der gefilterten Datei, auf welche die sentimentanalyse ausgeführt wird
    			System.out.println(y);
			y++;	
    			try {
					Files.delete(input_path);
				} catch (IOException e) {
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
	 						tweet_counter++;
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
		 StanfordCoreNLP Pipeline = new StanfordCoreNLP(props);
		 Stream<Path> e_stream = null;
		 try 
		 {
			e_stream = Files.walk(input_Dir);
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
			 if(!f.equals(input_Dir))
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
					 		Files.move(g, Paths.get(negatives.toString() + "//File_" + Integer.toString(i) + "_" + Integer.toString(z)));
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
		 try 
		 {
			 result = new FileWriter(result_Dir.toString() + "//Result.json");			//Pfad für das finale Ergebnis
		 } 
		 catch (IOException e5) 
		 {
			 e5.printStackTrace();
			 System.out.println("Debug1");
			 System.exit(9);
		 }
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
							 hate_tweets++;
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
    		}
    	}	  
	    try 
	    {
	    	result.close();
		} 
	    catch (IOException e6) 
	    {
	    	e6.printStackTrace();
			System.out.println("Debug13");
			System.exit(9);
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
		FileWriter trimmer = null;
	 	Object[] trim_array_temp = trim_stream.toArray();
 	    trim_stream.close();
 	    int y = 0;
 	    String temp;
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
					try {
						trimmer = new FileWriter(input_Dir.toString() + "//" + "trimmed" + Integer.toString(y) + ".json");
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					 while(scan.hasNextLine())
					 {
						 temp = scan.nextLine(); 
						 try {
							if(temp.length() > 14) 
							{
								trimmer.write(temp.trim().substring(14,temp.length()).replaceAll("\n"," ").replaceAll("\"}", "\n").replaceAll("^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]","").replaceAll("https:\\/\\/[a-zA-Z]*.[a-zA-Z]*\\/[a-zA-Z0-9]*", ""));					
							} 
						 } catch (IOException e) 
						 {
							e.printStackTrace();
							}
					 }
					y++;
					try {
						trimmer.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				 }
				 scan.close();
				 x.toFile().delete();
			 }
	    }
	 }
 }


