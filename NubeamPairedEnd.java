package nubeam;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;


public class NubeamPairedEnd {
	String Extension;
	
	public void setExtension (String a) {
		Extension = a;
		return;
	}
	
	public static List<Long> NubeamPE (JavaPairRDD<Tuple2<String,String>,Tuple2<String,String>> x) {
		// nubeam - index
		HashMap<Double,Long> map1 = new HashMap<>();
		// index - nubeam
		HashMap<Long,Double> map2 = new HashMap<>();
		List<Long> res = new ArrayList<>();
		long index = 0;
		for ( Tuple2<Tuple2<String,String>,Tuple2<String,String>> id_sequence : x.collect() ) {
			//read first sequence
			
			double Nubeam1 = Generic.BinaryToNubeam(Generic.serializza(id_sequence._1._2()));
			double Nubeam2 = Generic.BinaryToNubeam(Generic.serializza(id_sequence._2._2()));
			int check = 0;
			if (map1.containsKey(Nubeam1)) {
				check = check + 1;
			}
			if (map2.containsValue(Nubeam2)) {
				check = check + 1;
			}
			
			int dupcheck = 0; 
			//check if read-pair is duplicate
			if (Nubeam1 == Nubeam2) {
				dupcheck = 1;
			}
			//check if complementary are duplicate
			if (Nubeam1 == Generic.BinaryToNubeam(Generic.Complementare(id_sequence._2._2()))) {
				dupcheck = 1;
			}
			if (Generic.BinaryToNubeam(Generic.Complementare(id_sequence._1._2())) == Nubeam2) {
				dupcheck = 1;
			}			
			
			if ((check <= 1) & (dupcheck==0)) {
				map1.put(Nubeam1, index);
				map2.put(index, Nubeam2);
			}
			index += 1;
		}
		res.addAll(map1.values());
		return res;
		
	}
	
	
}
