package dbs;
import java.util.stream.*;
import java.util.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.arrow.vector.util.Text;
import org.apache.spark.SparkConf;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.*;
import java.io.*;
import org.apache.hadoop.io.*;
import java.nio.file.*;
import org.apache.commons.*;
import java.nio.file.Files;


public class Sentimental {
	static int filecount = 0;
	public static Path split_Dir = null;											//Input directory of the original large Json Files
	public static Path input_Dir = Paths.get("D:\\DBSProjekt\\Datein");						//Input directory of the Split up Json Files
	public static Path result_Dir = Paths.get("D:\\DBSProjekt\\Ergebnis");					//Output directory of the result file containing only the text from negative sentiment tweets without metadata
	public static Path temp_Dir_1 = Paths.get("D:\\DBSProjekt\\zu_pruefen");					//A temporary directory
	public static Path temp_Dir_2 = Paths.get("D:\\DBSProjekt\\zu_pruefen_neu\\");			//Directory for the filtered files before the sentiment analysis 
	public static Path filter_Dir = Paths.get("D:\\DBSProjekt\\FilterListen\\Homophobic.csv");	//The Input directory and file name of the list of words used as a filter
	public static Path temp_Dir_3 = Paths.get("D:\\DBSProjekt\\Output");							//Temporary output path for the sentiment analysis, containing still both positive and negative tweets
	public static Path negatives = Paths.get("D:\\DBSProjekt\\Negatives\\");			//Finaler outputpfad in dem die Negativen Sentiment Dateien gespeichert werden
	public static String neg = "sentiment: Negative):";		
	public static String veryneg = "sentiment: Very Negative):";	
	public static Scanner scan;
	 public static void main(String[] args) {
		filecount++;
		boolean error = false;
		try {
			Files.createDirectories(Paths.get(temp_Dir_1.toString() + "\\" + Integer.toString(filecount)));
		} catch (IOException e8) {
			e8.printStackTrace();
		}
		StanfordCoreNLP.OutputFormat.valueOf("JSON");									//settings für meine lokale maschine
	    System.setProperty("hadoop.home.dir", "C:\\hadoop-3.3.1");
	    SparkConf sparkConf = new SparkConf().setAppName("Hate_Speech_Filter");
	    sparkConf.setMaster("local[*]");
		FileWriter result = null;
		try {
			result = new FileWriter(result_Dir.toString() + "\\Result.json");			//Pfad für das finale Ergebnis
		} catch (IOException e5) {
			e5.printStackTrace();
			System.exit(9);
		}
	    System.setProperty("illegal-access", "permit");
	    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
	    Dataset<Row> filterdatei = sparkSession.read().option("inferSchema", true).option("sep",";").option("header", true).csv(filter_Dir.toString()); //die Optionen und der Pfad des Dictionaries mit dem gefiltert wird
	    filterdatei.createOrReplaceTempView("filter_liste");
	    Stream<Path> paths = null;
		try {
			paths = Files.walk(input_Dir);
		} catch (IOException e7) {
			e7.printStackTrace();
			System.exit(9);
		}

		Object[] temp = paths.toArray();
	    Path[] inputs = Arrays.copyOf(temp, temp.length, Path[].class);
	    int i = 1;
	    for(Path input_path : inputs)
	    {	
		    Dataset<Row> zu_pruefen = sparkSession.read().option("inferSchema", true).json(input_path.toString() + "\\" + "Datei" + Integer.toString(i)+ ".json"); //Pfad und name der einzulesenden Dateien hier "Datei(i).json"
		    zu_pruefen.createOrReplaceTempView("zu_pruefen_view");
		    filterdatei = sparkSession.sql("Select text from zu_pruefen_view , filter_liste WHERE zu_pruefen_view.text LIKE ('%' || ' ' || filter_liste.term || ' ' || '%') ");  //pruefen auf enthalten der Filterliste
		    filterdatei.write().json(temp_Dir_1.toString() + "\\" + Integer.toString(i));			//Output der gefilterten Datei, auf welche die sentimentanalyse ausgeführt wird
		    Path a = Paths.get(temp_Dir_1.toString() + "\\" + Integer.toString(i));
		    Path b = Paths.get(temp_Dir_2.toString() + "\\" + Integer.toString(i) + "\\");
		    try {
				Files.move(a,b);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(9);
			}
		    int y = 0;
	    	Stream<Path> b_stream = null;
			try {
				b_stream = Files.walk(b);
			} catch (IOException e6) {
				e6.printStackTrace();
				System.exit(9);
			}
	 	    Object[] b_array_temp = b_stream.toArray();
	 	    b_stream.close();
	 	    Path[] b_array = Arrays.copyOf(b_array_temp, b_array_temp.length, Path[].class);
		    for(Path d : b_array) {			//alle metadateien werden gelöscht die zu pruefenden dateien werden umbenannt in einem inkrementierenden schema
				if(d.toString().contains(".crc")) {
		    		try {
						Files.delete(d);
					} catch (IOException e1) {
						e1.printStackTrace();
						System.exit(9);
					}
		    	}
		    	else if(d.toString().contains("_SUCCESS")) {
		    		try {
						Files.delete(d);
					} catch (IOException e1) {
						e1.printStackTrace();
						System.exit(9);
					}
		    	}
		    	else if(d.toString().contains(".json"))
		    	{
		    		try {
						Files.move(d, Paths.get(temp_Dir_1.toString() + "\\" + Integer.toString(filecount) + "\\Datei_" + Integer.toString(i) + "_" + Integer.toString(y) + ".json"),StandardCopyOption.REPLACE_EXISTING);
					} catch (IOException e1) {
						e1.printStackTrace();
						System.exit(9);
					}
		    	    y++;
		    	}
		    }
		    Properties props = new Properties();
		    props.setProperty("annotators", "tokenize, ssplit, pos, parse,sentiment");
		    StanfordCoreNLP Pipeline = new StanfordCoreNLP(props);
		    Path e = Paths.get(temp_Dir_1.toString() + "\\" + Integer.toString(i) + "\\");
	    	Stream<Path> e_stream = null;
			try {
				e_stream = Files.walk(e);
			} catch (IOException e5) {
				e5.printStackTrace();
				System.exit(9);
			}
	 	    Object[] e_array_temp = e_stream.toArray();
	 	    e_stream.close();
	 	    Path[] e_array = Arrays.copyOf(e_array_temp, e_array_temp.length, Path[].class);
		    for(Path f : e_array)			// Die sentimentanalyse wird ausgeführt 
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
			   try {
			    	Pipeline.run();
			    } catch (IOException e1) {
			    	e1.printStackTrace();
					System.exit(9);
			    }
			    
			} 
	    	

	    	Path outputs = Paths.get(temp_Dir_3.toString());  // Filtern der Sentimentanalys auf negative und sehr negative ergebnisse
	    	int z = 0;
	    	if(!error) {
		    	Stream<Path> outputs_stream;
				try {
					outputs_stream = Files.walk(outputs);
				} catch (IOException e4) {
					e4.printStackTrace();
					break;
				}
		 	    Object[] outputs_array_temp = outputs_stream.toArray();
		 	    outputs_stream.close();
		 	    Path[] outputs_array = Arrays.copyOf(outputs_array_temp, outputs_array_temp.length, Path[].class);
		    	for(Path g : outputs_array) 
		    	{
		    		String temp_string;
		    		try {
						 scan = new Scanner(g);
						 while(scan.hasNextLine()) 
						 {
							 temp_string = scan.nextLine();
							 if(temp_string.contains(neg) || temp_string.contains(veryneg)) 
							 {
								 scan.close();
								 try 
								 {
									 Files.move(g, Paths.get(negatives.toString() + "\\File_" + Integer.toString(i) + "_" + Integer.toString(z)));
								 } 
								 catch (IOException e2) 
								 {
									e2.printStackTrace();
									System.exit(9);
								 }
								 break;
							 }
						 }
						scan.close();
						}
		    			catch (FileNotFoundException e3) 
			    			{
								e3.printStackTrace();
							} catch (IOException e1) {
							e1.printStackTrace();
							System.exit(9);
						}
		    			z++;
		    	}
	    	}
	    	
	    	}
    	Stream<Path> negatives_stream = null;
		try {
			negatives_stream = Files.walk(negatives);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(9);
		}
 	    Object[] negatives_array_temp = negatives_stream.toArray();
 	    negatives_stream.close();
 	    Path[] negatives_array = Arrays.copyOf(negatives_array_temp, negatives_array_temp.length, Path[].class);
	    for(Path h : negatives_array)
	    	{
		    	String temp_string;
		    		try {
						scan = new Scanner(h);
					} catch (FileNotFoundException e4) {
						System.out.print("Fehler beim einscannen der Negativen sentiment dateien");
						e4.printStackTrace();
						System.exit(9);
					} catch (IOException e) {
						e.printStackTrace();
						System.exit(9);
					}
		    		
					 while(scan.hasNextLine()) {
						 temp_string = scan.nextLine();
						 if(temp_string.contains(neg) || temp_string.contains(veryneg)) {
							 try {
								result.write(scan.nextLine() + "\n");
							} catch (Exception e5) {
								e5.printStackTrace();
								System.exit(9);
							}
						 }
	
					 }
					 scan.close();
		    	}	  

	    
		
	    try {
			result.close();
		} catch (IOException e6) {
			e6.printStackTrace();
			System.exit(9);
		}
	 }
}
