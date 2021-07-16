package nubeam;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;



import fastdoop.FASTAlongInputFileFormat;
import fastdoop.FASTQInputFileFormat;
import fastdoop.PartialSequence;
import fastdoop.QRecord;
import scala.Function;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple6;


public class Nubeam {
	
	public static Boolean Filter(Tuple2<Tuple2<String,String>,Long> x,List<Long> y) {
		if (y.contains(x._2())) {
			return true;
		}
		return false;
	}
	public static Boolean InverseFilter(Tuple2<Tuple2<String,String>,Long> x,List<Long> y) {
		if (y.contains(x._2())) {
			return false;
		}
		return true;
	}
	public static Boolean SampleFilter(Tuple2<Tuple2<String,String>,Long> x,Integer n) {
		if(x._2 <= n) {
			return true;
		}
		return false;
	}
	public static Boolean FilterAlt(Tuple2<String,Long> x , Long n) {
		if(x._2 <= n ) {
			return true;
		}
		return false;
	}
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\hadoop" );
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf sc = new SparkConf().set("spark.driver.maxResultSize", "4g");
		sc.setAppName("Progetto: Nubeam-dedup");
		sc.setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		
		//Interazione con utente
		
		Scanner input = new Scanner(System.in);
		System.out.println("Nubeam - Genetic Sequence Deduplication");
		System.out.println("Choose Reads Type : Single-End or Paired-End");
		System.out.println("Insert : -se for Single-End -pe for Paired-End");
		String ReadType = input.next();
		while ((ReadType.contains("se")==false) & (ReadType.contains("pe")==false)) {
			System.out.println("Incorrect Input - Please try again");
			ReadType = input.next();
		}
		// ----------------------------   SINGLE END    --------------------
		if (ReadType.contains("se")) {
			System.out.println("File Extension : FASTQ supported");
			System.out.println("Insert name of file : (File has to be inside the data folder in the program directory)");
			String File = input.next();
			System.out.println("Do you want to save De-Duplicated Files? (y for yes n for no)");
			String Answer1 = input.next();
			System.out.println("Do you want to save Removed Sequences to File? (y for yes n for no)");
			String Answer2 = input.next();
			System.out.println("Do you want to run the program on a sample of the data? (y for yes n for no)");
			String Answer3 = input.next();
			
			/*if (Extension == "FASTA") {
				Configuration inputConf = jsc.hadoopConfiguration();
				inputConf.setInt("k", 50);

				String inputPath1 = "data/" + File;

				JavaPairRDD<Text, PartialSequence> dSequences1 = jsc.newAPIHadoopFile(inputPath1, 
						FASTAlongInputFileFormat.class, Text.class, PartialSequence.class, inputConf);
				JavaPairRDD<String, String> dSequences_1 = dSequences1.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));

			}*/
			
			Configuration inputConf = jsc.hadoopConfiguration();
			inputConf.setInt("look_ahead_buffer_size", 2048);
			String inputPath1 = "data/" + File;
			long startTime = System.currentTimeMillis();
			JavaPairRDD<Text, QRecord> dSequences1 = jsc.newAPIHadoopFile(inputPath1, 
						FASTQInputFileFormat.class, Text.class, QRecord.class, inputConf);
			
			
			JavaPairRDD<String, String> dSequences_1 = dSequences1.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));
			dSequences1.unpersist();
			if (Answer3.contains("y")) {
				System.out.println("Input Number (from 1 to 100) - Percentage of data you wish to sample");
				int Num1 = input.nextInt();
				long perc = (Num1*dSequences_1.count())/100;
				JavaPairRDD<String,String> sampledSeq = dSequences_1.sample(false, perc, 101);
				dSequences_1.unpersist();
				List<Long> Index = NubeamSingleEnd.NubeamSE(sampledSeq);
				JavaPairRDD<String,String> Result = sampledSeq.zipWithIndex().filter(record -> Filter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
				long endTime = System.currentTimeMillis();
				long searchTime = endTime - startTime;
				if (Answer1.contains("y")) {
					System.out.println("Choose File Name for De-Duplicated File : ");
					String OutFile = input.next();
					Result.saveAsTextFile("data/" + OutFile);
				}
				if (Answer2.contains("y")) {
					System.out.println("Choose File Name for Removed Sequences : ");
					String Removed = input.next();
					JavaPairRDD<String,String> Removed1 = dSequences_1.zipWithIndex().filter(record -> InverseFilter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
					Removed1.saveAsTextFile("data/"+Removed);
					
				}
				System.out.println("Original Number of Sequences : " + sampledSeq.count());
				System.out.println("Unique Sequences : "+ Result.count());
				System.out.println("Removed Sequences : " + (sampledSeq.count() - Result.count()));
				System.out.println("CPU Time (seconds) : " + searchTime/1000);	
			}
			if (Answer3.contains("n")) {
				List<Long> Index = NubeamSingleEnd.NubeamSE(dSequences_1);
				JavaPairRDD<String,String> Result = dSequences_1.zipWithIndex().filter(record -> Filter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
			    long endTime = System.currentTimeMillis();
			    long searchTime = endTime - startTime;
			    if (Answer1.contains("y")) {
					System.out.println("Choose File Name for De-Duplicated File : ");
					String OutFile = input.next();
					SaveFile.SaveToFQ(Result, OutFile );
				}
				if (Answer2.contains("y")) {
					System.out.println("Choose File Name for Removed Sequences : ");
					String Removed = input.next();
					JavaPairRDD<String,String> Removed1 = dSequences_1.zipWithIndex().filter(record -> InverseFilter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
					SaveFile.SaveToFQ(Removed1, Removed );
					
				}
				System.out.println("Original Number of Sequences : " + dSequences_1.count());
				long a = Result.count() ;
				System.out.println("Unique Sequences : "+ a);
				System.out.println("Removed Sequences : " + (dSequences_1.count() - a));
				System.out.println("CPU Time (seconds) : " + searchTime/1000);
			}
			
		}
		
		// ------------------------------------- PAIRED END ---------------------------------
		
		else if (ReadType.contains("pe")) {
			System.out.println("File Extension : FASTQ supported");
			
			System.out.println("Insert name of the files : (File has to be inside the data folder in the program directory)");
			String File1 = input.next();
			System.out.println("Insert name of second file :");
			String File2 = input.next();
			System.out.println("Do you want to save De-Duplicated Files? (y for yes n for no)");
			String Answer1 = input.next();
			System.out.println("Do you want to save Removed Sequences to File? (y for yes n for no)");
			String Answer2 = input.next();
			System.out.println("Do you want to run the program on a sample of the data? (y for yes n for no)");
			String Answer3 = input.next();
			/*if (ExtensionPE.contains("FASTA") ) {
				Configuration inputConf = jsc.hadoopConfiguration();
				inputConf.setInt("k", 50);

				String inputPath1 = "data/" + File1;
				String inputPath2 = "data/" + File2;

				JavaPairRDD<Text, PartialSequence> dSequences1 = jsc.newAPIHadoopFile(inputPath1, 
						FASTAlongInputFileFormat.class, Text.class, PartialSequence.class, inputConf);
				JavaPairRDD<Text, PartialSequence> dSequences2 = jsc.newAPIHadoopFile(inputPath2, 
						FASTAlongInputFileFormat.class, Text.class, PartialSequence.class, inputConf);
				JavaPairRDD<String, String> dSequences_1 = dSequences1.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));
				JavaPairRDD<String, String> dSequences_2 = dSequences2.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));

			}*/
			
			Configuration inputConf = jsc.hadoopConfiguration();
			inputConf.setInt("look_ahead_buffer_size", 2048);
			String inputPath1 = "data/" + File1;
			String inputPath2 = "data/" + File2;
			long startTime = System.currentTimeMillis();
			//sample
			/*
			JavaRDD<String> one = jsc.textFile(inputPath1);
			JavaRDD<String> two = jsc.textFile(inputPath2);
			long a = 20000000;
			JavaRDD<String> onezipped = one.zipWithIndex().filter(rec -> FilterAlt(rec,a)).map(record -> record._1);
			JavaRDD<String> twozipped = two.zipWithIndex().filter(rec -> FilterAlt(rec,a)).map(record -> record._1);
			SaveFile.SaveToFQ_RDD(onezipped, "LargeSample1");
			SaveFile.SaveToFQ_RDD(twozipped, "LargeSample2");
			System.exit(0);
			*/
			JavaPairRDD<Text, QRecord> dSequences1 = jsc.newAPIHadoopFile(inputPath1, 
						FASTQInputFileFormat.class, Text.class, QRecord.class, inputConf);
			JavaPairRDD<Text, QRecord> dSequences2 = jsc.newAPIHadoopFile(inputPath2, 
						FASTQInputFileFormat.class, Text.class, QRecord.class, inputConf);
		
			JavaPairRDD<String, String> dSequences_1 = dSequences1.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));
			JavaPairRDD<String, String> dSequences_2 = dSequences2.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));
			
			
			if(Answer3.contains("y")) {
				//sample
				System.out.println("Choose Percentage to sample (integer from 1 to 100) : ");
				int sample = input.nextInt();
				int size = (int) dSequences_1.count();
				int sample_size = (sample*size)/100;
				
				JavaPairRDD<String,String> dSeq1 = dSequences_1.zipWithIndex().filter(record -> SampleFilter(record,sample_size)).mapToPair(record -> new Tuple2<>(record._1._1,record._1._2));
				JavaPairRDD<String,String> dSeq2 = dSequences_2.zipWithIndex().filter(record -> SampleFilter(record,sample_size)).mapToPair(record -> new Tuple2<>(record._1._1,record._1._2));
				JavaPairRDD<Tuple2<String,String>,Tuple2<String,String>> zipped = dSeq1.coalesce(1).zip(dSeq2.coalesce(1));

				//List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> list = Generic.zipJava8(dSeq1.collect(), dSeq2.collect());
				dSequences1.unpersist();
				dSequences2.unpersist();
				dSequences_1.unpersist();
				dSequences_2.unpersist();
				
				//JavaPairRDD<Tuple2<String,String>,Tuple2<String,String>> Combined = jsc.parallelizePairs(list);
				List<Long> Index = NubeamPairedEnd.NubeamPE(zipped);
				long deDupSequences = Index.size();
				
				//Combined.unpersist();
				//list.clear();
				JavaPairRDD<String,String> Dedup1 = dSeq1.zipWithIndex().filter(record -> Filter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
				JavaPairRDD<String,String> Dedup2 = dSeq2.zipWithIndex().filter(record -> Filter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
				
				long endTime = System.currentTimeMillis();
			    long searchTime = endTime - startTime;
			    if (Answer1.contains("y")) {
					System.out.println("choose name for De-Duplicated file 1 :");
					String OutFile1 = input.next();
					System.out.println("choose name for De-Duplicated file 2 :");
					String OutFile2 = input.next();
					SaveFile.SaveToFQ(Dedup1, OutFile1 );
					SaveFile.SaveToFQ(Dedup2, OutFile2 );
	
				}
				if (Answer2.contains("y")) {
					System.out.println("choose name for Removed Sequences file 1 :");
					String RemFile1 = input.next();
					System.out.println("choose name for Removed Sequences file 2 :");
					String RemFile2 = input.next();
					JavaPairRDD<String,String> Removed1 = dSequences_1.zipWithIndex().filter(record -> InverseFilter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
					JavaPairRDD<String,String> Removed2 = dSequences_2.zipWithIndex().filter(record -> InverseFilter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
					SaveFile.SaveToFQ(Removed1, RemFile1 );
					SaveFile.SaveToFQ(Removed2, RemFile2 );
					
				}
				long origlen = dSeq1.count();
				System.out.println("Original Number of Sequences : " + origlen);
				System.out.println("De-Duplicated Number of Sequences : "+ deDupSequences);
				System.out.println("Removed Sequences : " + (origlen - deDupSequences));
				System.out.println((deDupSequences/2) + "/" + origlen + " sequences are unique.");
				System.out.println("CPU Time (seconds) : "+searchTime/1000);
			}
			//System.out.println(dSequences_2.partitions());
			if(Answer3.contains("n")) {
				JavaPairRDD<Tuple2<String,String>,Tuple2<String,String>> zipped = dSequences_1.coalesce(1).zip(dSequences_2.coalesce(1));

				//List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> list = Generic.zipJava8(dSequences_1.collect(), dSequences_2.collect());
				dSequences1.unpersist();
				dSequences2.unpersist();
				//JavaPairRDD<Tuple2<String,String>,Tuple2<String,String>> Combined = jsc.parallelizePairs(list);
				List<Long> Index = NubeamPairedEnd.NubeamPE(zipped);
				long deDupSequences = Index.size();
				
				//Acquisizione Dati Neo4j
				/*
				List<Tuple6<String,String,Double,String,String,Double>> pp = Neo4jData.DataPrep(Combined);
				System.out.println(pp.subList(0, 10));
				SaveFile.SaveListToTxt(pp, "ListaSmallSample");
				System.exit(0);
				*/
				//
				
				//Combined.unpersist();
				//list.clear();
				JavaPairRDD<String,String> Dedup1 = dSequences_1.zipWithIndex().filter(record -> Filter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
				JavaPairRDD<String,String> Dedup2 = dSequences_2.zipWithIndex().filter(record -> Filter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
				dSequences_1.unpersist();
				dSequences_2.unpersist();
				long endTime = System.currentTimeMillis();
			    long searchTime = endTime - startTime;

				//Write them to FastQ File
				if (Answer1.contains("y")) {
					System.out.println("choose name for De-Duplicated file 1 :");
					String OutFile1 = input.next();
					System.out.println("choose name for De-Duplicated file 2 :");
					String OutFile2 = input.next();
					SaveFile.SaveToFQ(Dedup1, OutFile1 );
					SaveFile.SaveToFQ(Dedup2, OutFile2 );
				}
				if (Answer2.contains("y")) {
					System.out.println("choose name for Removed Sequences file 1 :");
					String RemFile1 = input.next();
					System.out.println("choose name for Removed Sequences file 2 :");
					String RemFile2 = input.next();
					JavaPairRDD<String,String> Removed1 = dSequences_1.zipWithIndex().filter(record -> InverseFilter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
					JavaPairRDD<String,String> Removed2 = dSequences_2.zipWithIndex().filter(record -> InverseFilter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
					SaveFile.SaveToFQ(Removed1, RemFile1 );
					SaveFile.SaveToFQ(Removed2, RemFile2 );
				}
				long origlen = dSequences_1.count();
				System.out.println("Original Number of Sequences : " + origlen);
				System.out.println("De-Duplicated Number of Sequences : "+ deDupSequences);
				System.out.println("Removed Sequences : " + (origlen - deDupSequences));
				System.out.println((deDupSequences/2) + "/" + origlen + " sequences are unique.");
				System.out.println("CPU Time (seconds) : "+searchTime/1000);
			}
			
			/*JavaRDD<String> Zipped = dSequences_1.zipPartitions(dSequences_2 , new FlatMapFunction2<Iterator<Tuple2<String,String>>,Iterator<Tuple2<String,String>>,String>() {
				
				private static final long serialVersionUID = 0;

				@Override
				public Iterator<String> call(Iterator<Tuple2<String,String>> arg0,Iterator<Tuple2<String,String>> arg1) throws Exception {
					List<String> res = new ArrayList<>();
					while((arg0.hasNext()) && (arg1.hasNext())) {
						res.add(arg1.next()._1 + "//" + arg1.next()._2 + "//" + arg0.next()._1 +"//"+arg0.next()._2);
					}
					
					return res.iterator();
				}
			});
			
			System.out.println(Zipped.take(6));*/
			
		}
		jsc.close();
		input.close();
		}

}
