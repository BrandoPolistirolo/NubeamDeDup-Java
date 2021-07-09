package nubeam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

public class NubeamSingleEnd {
	
	public static List<Long> NubeamSE(JavaPairRDD<String,String> x ) {
		HashMap<Double,Long> tempmap = new HashMap<>();
		List<Long> indexes = new ArrayList<>();
		long index = 0;
		long reversedindex = -1;
		for (Tuple2<String,String> sequence : x.collect()) {
			double Nubeam = Generic.BinaryToNubeam(Generic.serializza(sequence._2()));
			if (tempmap.containsKey(Nubeam)) {
				continue;
			}
			tempmap.put(Nubeam, index);
			String ReversedSequence = Generic.reverseString(sequence._2());
			double NubeamReversedComplement = Generic.BinaryToNubeam(Generic.serializza(ReversedSequence));
			tempmap.put(NubeamReversedComplement,reversedindex);
			index = index + 1;
		}
		//take out keys with index equal to -1
		indexes.addAll(tempmap.values());
		indexes.removeIf(n ->  (n == -1) );
		return indexes;
	}

}
