package nubeam;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;


import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import scala.Tuple2;


public class NubeamPairedEnd {
	
	
	
	public static List<Long> NubeamPE (JavaPairRDD<Tuple2<String,String>,Tuple2<String,String>> x) {
		// nubeam - index
		Multimap<Double, Long> map = ArrayListMultimap.create();

		//HashMap<Double, Long> map1 = new HashMap<>();
		// index - nubeam
		HashMap<Long,Double> map2 = new HashMap<>();
		List<Long> res = new ArrayList<>();
		long index = 0;
		for ( Tuple2<Tuple2<String,String>,Tuple2<String,String>> id_sequence : x.collect() ) {
			//read first sequence
			
			double Nubeam1 = Generic.BinaryToNubeam(Generic.serializza(id_sequence._1._2()));
			double Nubeam2 = Generic.BinaryToNubeam(Generic.serializza(id_sequence._2._2()));
			int check1 = 0;
			if (map.containsKey(Nubeam1)) {
				for(Long ind : map.get(Nubeam1)) {
					if(map2.get(ind).equals(Nubeam2)) {
						check1 = 1;
					}
				}
			}
			if (check1 == 1) continue;
			/*if (map1.containsKey(Nubeam1)) {
				
				long ind = map1.get(Nubeam1);
				if (map2.get(ind).equals(Nubeam2)) {
					continue;
				}
			}*/
			// check if read-pair from complementary strand is duplicate
			double Nubeam1Complementary = Generic.BinaryToNubeam(Generic.serializza(Generic.Complementare(id_sequence._1._2())));
			double Nubeam2Complementary = Generic.BinaryToNubeam(Generic.serializza(Generic.Complementare(id_sequence._2._2())));
			int check2 = 0;
			if(map.containsKey(Nubeam1Complementary)) {
				for(Long ind : map.get(Nubeam1Complementary)) {
					if(map2.get(ind).equals(Nubeam2Complementary)) {
						check2 = 1;
					}
				}
			}
			if (check2 == 1) continue;
			/*if (map1.containsKey(Nubeam1Complementary)) {
				
				long ind1 = map1.get(Nubeam1Complementary);
				if (map2.get(ind1).equals(Nubeam2Complementary)) {

					continue;
				}
			}*/
			map.put(Nubeam1, index);
			//map1.put(Nubeam1, index);
			map2.put(index, Nubeam2);
			index += 1;
		}
		res.addAll(map.values());
		
		//res.addAll(map1.values());
		map.clear();
		//map1.clear();
		map2.clear();
		return res;
		
	}
	
	
}
