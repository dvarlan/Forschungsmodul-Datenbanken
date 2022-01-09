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


public class Sentimental {
	static int filecount = 0;
	public static Path split_Dir = Paths.get("/home/ubuntu/Raw_Data/");											//Input directory of the original large Json Files
	public static Path input_Dir = Paths.get("/home/ubuntu/Datein/");						//Input directory of the Split up Json Files or any empty directory if Files are split automatically
	public static Path result_Dir = Paths.get("/home/ubuntu/Ergebnis");					//Output directory of the result file containing only the text from negative sentiment tweets without metadata
	public static Path temp_Dir_1 = Paths.get("/home/ubuntu/zu_pruefen");					//A temporary directory
	public static Path filter_Dir = Paths.get("/home/ubuntu/FilterListen//Ethnic.csv");	//The Input directory and file name of the list of words used as a filter
	public static Path temp_Dir_3 = Paths.get("/home/ubuntu/Output");							//Temporary output path for the sentiment analysis, containing still both positive and negative tweets
	public static Path negatives = Paths.get("/home/ubuntu/Negatives/");			//Finaler outputpfad in dem die Negativen Sentiment Dateien gespeichert werden
	public static String neg = "sentiment: Negative):";
	public static String veryneg = "sentiment: Very Negative):";
	public static Scanner scan;
	public static long tweet_counter = 0;
	public static long hate_tweets = 0;
	
	 public static void main(String[] args) {
		long unixstart = Instant.now().getEpochSecond();
		split();
		cross_reference();
		meta_delete();
		analyse_sentiment();
		move_negatives();
		write_result();
	    runtime(unixstart);
	    //System.out.print(((int) (tweet_counter) / hate_tweets!=0?hate_tweets:1) + "%");
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
    			filterdatei = sparkSession.sql("Select text from zu_pruefen_view , filter_liste WHERE zu_pruefen_view.text LIKE ('%' || ' ' || filter_liste.term || ' ' || '%') ");  //pruefen auf enthalten der Filterliste
    			filterdatei.write().json(temp_Dir_1.toString() + "//" + Integer.toString(y));			//Output der gefilterten Datei, auf welche die sentimentanalyse ausgeführt wird
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
		 StanfordCoreNLP Pipeline = new StanfordCoreNLP(props);
		 Stream<Path> e_stream = null;
		 try 
		 {
			e_stream = Files.walk(temp_Dir_1);
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
			 if(!f.equals(temp_Dir_1))
			 {
				 props.setProperty("file", f.toString());
				 props.setProperty("-pos.model","edu/stanford/nlp/models/pos-tagger/english-bidirectional-distsim.tagger");
				 props.setProperty("tokenize.options", "untokenizable = allDelete");
				 props.setProperty("tokenize.options", "tokenizePerLine = true");
				 props.setProperty("tokenize.options", "ud = true");
				 props.setProperty("tokenize.options", "normalizeAmpersandEntity = true");
				 props.setProperty("tokenize.options", "normalizeCurrency = true");
				 props.setProperty("tokenize.options", "quotes = unicode");
				 props.setProperty("tokenize.options", "splitAssimilations = false");
				 props.setProperty("tokenize.options", "ellipses = unicode");
				 props.setProperty("tokenize.options", "tokenizeNLs = false");
				 props.setProperty("tokenize.options", "normalizeParentheses = true"); 
				 props.setProperty("outputDirectory", temp_Dir_3.toString()); // Speicherort der Sentimentanalyse
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
 }


