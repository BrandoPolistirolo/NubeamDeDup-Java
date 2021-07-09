package nubeam;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
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

import scala.Tuple2;


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
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\hadoop" );
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkConf sc = new SparkConf();
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
		if (ReadType.contains("se")) {
			System.out.println("File Extension : FASTQ supported");
			System.out.println("Insert name of file : (File has to be inside the data folder in the program directory)");
			String File = input.next();
			System.out.println("Do you want to save De-Duplicated Files? (y for yes n for no)");
			String Answer1 = input.next();
			System.out.println("Do you want to save Removed Sequences to File? (y for yes n for no)");
			String Answer2 = input.next();
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

			JavaPairRDD<Text, QRecord> dSequences1 = jsc.newAPIHadoopFile(inputPath1, 
						FASTQInputFileFormat.class, Text.class, QRecord.class, inputConf);
				
			JavaPairRDD<String, String> dSequences_1 = dSequences1.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));
			List<Long> Index = NubeamSingleEnd.NubeamSE(dSequences_1);
			JavaPairRDD<String,String> Result = dSequences_1.zipWithIndex().filter(record -> Filter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
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
			System.out.println("Original Number of Sequences : " + dSequences_1.count());
			System.out.println("Unique Sequences : "+ Result.count());
			System.out.println("Removed Sequences : " + (dSequences_1.count() - Result.count()));
			
			
		}
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
				
			JavaPairRDD<Text, QRecord> dSequences1 = jsc.newAPIHadoopFile(inputPath1, 
						FASTQInputFileFormat.class, Text.class, QRecord.class, inputConf);
			JavaPairRDD<Text, QRecord> dSequences2 = jsc.newAPIHadoopFile(inputPath2, 
						FASTQInputFileFormat.class, Text.class, QRecord.class, inputConf);
			JavaPairRDD<String, String> dSequences_1 = dSequences1.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));
			JavaPairRDD<String, String> dSequences_2 = dSequences2.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));
			List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> list = Generic.zipJava8(dSequences_1.collect(), dSequences_2.collect());
			JavaPairRDD<Tuple2<String,String>,Tuple2<String,String>> Combined = jsc.parallelize(list).mapToPair(record -> new Tuple2<>(record._1,record._2));
			List<Long> Index = NubeamPairedEnd.NubeamPE(Combined);
				
			JavaPairRDD<String,String> Dedup1 = dSequences_1.zipWithIndex().filter(record -> Filter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
			JavaPairRDD<String,String> Dedup2 = dSequences_2.zipWithIndex().filter(record -> Filter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
			
			//Write them to FastQ File
			if (Answer1.contains("y")) {
				System.out.println("choose name for De-Duplicated file 1 :");
				String OutFile1 = input.next();
				System.out.println("choose name for De-Duplicated file 2 :");
				String OutFile2 = input.next();
				Dedup1.saveAsTextFile("data/"+OutFile1+".fq");
				Dedup2.saveAsTextFile("data/"+OutFile2+".fq");
			}
			if (Answer2.contains("y")) {
				System.out.println("choose name for Removed Sequences file 1 :");
				String RemFile1 = input.next();
				System.out.println("choose name for Removed Sequences file 2 :");
				String RemFile2 = input.next();
				JavaPairRDD<String,String> Removed1 = dSequences_1.zipWithIndex().filter(record -> InverseFilter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
				JavaPairRDD<String,String> Removed2 = dSequences_2.zipWithIndex().filter(record -> InverseFilter(record,Index)).mapToPair(record -> new Tuple2<>(record._1._1(),record._1._2()));
				Removed1.saveAsTextFile("data/"+RemFile1);
				Removed2.saveAsTextFile("data/"+RemFile2);
			}
			long origlen = dSequences_1.count();
			System.out.println("Original Number of Sequences : " + origlen);
			long deduplen = Dedup1.count();
			System.out.println("De-Duplicated Number of Sequences : "+deduplen);
			long removedseq = origlen - deduplen;
			System.out.println("Removed Sequences : " + removedseq);
			long unique = origlen - removedseq;
			System.out.println(unique + "/" + dSequences_1.count() + " sequences are unique.");
		}
		jsc.close();
		input.close();
		//Acquisizione dati nuova
		
		/*
		String inputPath1 = "data/1.fq";
		String inputPath2 = "data/2.fq";
		
		JavaPairRDD<Text, QRecord> dSequences1 = jsc.newAPIHadoopFile(inputPath1, 
				FASTQInputFileFormat.class, Text.class, QRecord.class, inputConf);
		JavaPairRDD<Text, QRecord> dSequences2 = jsc.newAPIHadoopFile(inputPath2, 
				FASTQInputFileFormat.class, Text.class, QRecord.class, inputConf);
		/* We drop the keys of the new RDD since they are not used, than a PairRDD (ID, sequence) is created */
		//JavaPairRDD<String, String> dSequences_1 = dSequences1.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));
		//JavaPairRDD<String, String> dSequences_2 = dSequences2.values().mapToPair(record -> new Tuple2<>(record.getKey(), record.getValue()));
		 
		
		/*
		for (Tuple2<String, String> id_sequence : dSequences.collect()) {
			System.out.println("ID: " + id_sequence._1);
			System.out.println("Sequence: " + id_sequence._2);
		}
		*/
		//Acquisizione dati
		
		
		//Single End Reads
		/*
		JavaRDD<String> dLines1 = jsc.textFile("data/1.fq");
		JavaRDD<String> singleend = dLines1.mapToPair(x -> Counter(x)).filter(x -> FiltroSingleEnd(x)).map(x -> x._1);
		
		List<Double> nubeamdedup = new ArrayList<Double>(); 
		for(String x : singleend.collect()) {
			double nubeam = SerializzatoreBinario.BinaryToNubeam(SerializzatoreBinario.serializza(x));
			if (nubeamdedup.contains(nubeam)==false) {
				nubeamdedup.add(nubeam);
				nubeamdedup.add(SerializzatoreBinario.BinaryToNubeam(SerializzatoreBinario.serializza(SerializzatoreBinario.Complementare(x))));
			}
		}
		System.out.println(singleend.count());
		System.out.println(nubeamdedup.size());
		jsc.close();
		*/
		//JavaRDD<Double> distinct = Nubeam.distinct();
		
		//JavaPairRDD<String,Double> Nubeam1 = Nubeam.distinct();
		//prova
		//System.out.println(nubeamdedup);
		
		//Paired End Reads
		

		
		//JavaRDD<String> dLines2 = jsc.textFile("data/2.fq");
		
		
		//JavaPairRDD<Object> binary = filtered.map(x,y -> CalcolaNubeam(x,y));
		
		
			
		//System.out.println(dLines1.collect());
		
		
		/*String uri = "bolt://localhost:7687";
		AuthToken token = AuthTokens.basic("neo4j", "ciaociao");
		Driver driver = GraphDatabase.driver(uri, token);
		Session s = driver.session();	
		System.out.println("Connessione stabilita!");
		*/
		
		
		/*Prova Calcolo Nubeam Number
		String prova1 = "ATCAGC";
		String prova2 = "ATGAGC";
		String binaryprova1 = SerializzatoreBinario.serializza(prova1);
		String binaryprova2 = SerializzatoreBinario.serializza(prova2);
		double nubeam1 = SerializzatoreBinario.BinaryToNubeam(binaryprova1);
		double nubeam2 = SerializzatoreBinario.BinaryToNubeam(binaryprova2);
		System.out.println(binaryprova1);
		System.out.println(binaryprova2);
		System.out.printf("nubeam1: %f\n", nubeam1);
		System.out.printf("nubeam2: %f\n", nubeam2);
		*/
		//Prova Calcolo Nubeam
		/*
		String binaryprova3 = SerializzatoreBinario.serializza(prova3);
		double nubeamprova3 = SerializzatoreBinario.BinaryToNubeam(binaryprova3);
		System.out.printf("nubeam1: %f\n", nubeamprova3);
		*/
		
		/*prova complementare
		String prova3 = "CTCACCGTAAAAATATGCTGCATCGGCAAACCAGTAAACGTACTGCGCGGCCACTGATCCTGCTCAATACCGGGACCAGTCGAACCCACCCAGCCAGCCT";
		String complementareprova3 = SerializzatoreBinario.Complementare(prova3);
		System.out.println(prova3);
		System.out.println(complementareprova3);
		*/
		//String prova3 = "CTCACCGTAAAAATATGCTGCATCGGCAAACCAGTAAACGTACTGCGCGGCCACTGATCCTGCTCAATACCGGGACCAGTCGAACCCACCCAGCCAGCCT";
		//System.out.println(SerializzatoreBinario.BinaryToNubeam(SerializzatoreBinario.serializza(prova3)));
	}

}
