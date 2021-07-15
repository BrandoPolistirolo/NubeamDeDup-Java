package nubeam;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;
import scala.Tuple6;

public class Neo4jData {

	public static List<Tuple6<String,String,Double,String,String,Double>> DataPrep(JavaPairRDD<Tuple2<String,String>,Tuple2<String,String>> x) {
		List<Tuple6<String,String,Double,String,String,Double>> result = new ArrayList<Tuple6<String,String,Double,String,String,Double>>() ;
		for(Tuple2<Tuple2<String,String>,Tuple2<String,String>> rec : x.collect()) {
			result.add(new Tuple6<>(rec._1._1,
					rec._1._2,
					Generic.BinaryToNubeam(Generic.serializza(rec._1._2)),
					rec._2._1,
					rec._2._2,
					Generic.BinaryToNubeam(Generic.serializza(rec._2._2))
					));
		}
		return result;
		
	}
}
