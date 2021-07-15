package nubeam;
import java.io.File;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;
import scala.Tuple6;





public class SaveFile {
	public static void SaveToFQ(JavaPairRDD<String,String> j,String filename) {
		try {
		      PrintStream fileStream = new PrintStream(new File(filename + ".fq"));
		      for(Tuple2<String, String> rec : j.collect()) {
		    	  fileStream.println(rec._1());
		    	  fileStream.println(rec._2());
		      }
		      fileStream.close();
		      System.out.println("Successfully wrote to the file.");
		    } catch (IOException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
		
	}
	public static void SaveToFQ_RDD(JavaRDD<String> j,String filename) {
		try {
		      PrintStream fileStream = new PrintStream(new File(filename + ".fq"));
		      for(String rec : j.collect()) {
		    	  fileStream.println(rec); 
		      }
		      fileStream.close();
		      System.out.println("Successfully wrote to the file.");
		    } catch (IOException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
		
	}
	public static void SaveListToTxt(List<Tuple6<String,String,Double,String,String,Double>> k,String filename) {
		try {
		      PrintStream fileStream = new PrintStream(new File(filename + ".txt"));
		      Iterator<Tuple6<String,String,Double,String,String,Double>> it = k.listIterator(0);
		      while(it.hasNext()) {
		    	  Tuple6<String,String,Double,String,String,Double> temp = it.next();
		    	  fileStream.println(temp._1());
		    	  fileStream.println(temp._2());
		    	  fileStream.println(temp._3());
		    	  fileStream.println(temp._4());
		    	  fileStream.println(temp._5());
		    	  fileStream.println(temp._6());
		      }
		      fileStream.close();
		      System.out.println("Successfully wrote to the file.");
		    } catch (IOException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
	}

}
