package nubeam;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;



import scala.Tuple2;
import scala.Tuple3;

public class Generic {
	public static String serializza(String f) {
		String a = f.replace("A", "1");
		a = a.replace("T", "0");
		a = a.replace("G", "0");
		a = a.replace("C", "0");
		a = a.replace("N", "0");
		String t = f.replace("T", "1");
		t = t.replace("A", "0");
		t = t.replace("G", "0");
		t = t.replace("C", "0");
		t = t.replace("N", "0");
		String g = f.replace("G", "1");
		g = g.replace("A", "0");
		g = g.replace("T", "0");
		g = g.replace("C", "0");
		g = g.replace("N", "0");
		String c = f.replace("C", "1");
		c = c.replace("A", "0");
		c = c.replace("T", "0");
		c = c.replace("G", "0");
		c = c.replace("N", "0");
		
		return a+t+c+g;
	}
	static double[][] ones = {
			{1,1},
			{0,1}
	};
	static double[][] zeros = {
			{1,0},
			{1,1}
	};
	static double[][] weights = {
			{1,Math.sqrt(3)},
			{Math.sqrt(2),Math.sqrt(5)}
	};
	public static double CalcolaTraccia(double[][] x) {
		double traccia = x[0][0] + x[1][1];
		return traccia;
	}
	public static double[][] ProdottoMatriciQuadrate(double[][] a,double[][] b,int size) {
		double[][] product = new double[size][size];
		for(int j=0;j<size;j++) {
			for(int l=0;l<size;l++) {
				for(int p=0;p<size;p++) {
					product[j][l] += a[j][p]*b[p][l];
				}
			}
		}
		return product;
	}
	public static double BinaryToNubeam(String h) {
		double[][] k = new double[2][2];
		if (h.charAt(0) == '1') k = ones; 
		if (h.charAt(0) == '0') k = zeros;
		for (int i=1; i<h.length();i++) {
			if (h.charAt(i) == '1') k = ProdottoMatriciQuadrate(k,ones,2); 
			if (h.charAt(i) == '0') k = ProdottoMatriciQuadrate(k,zeros,2);
		}
		//System.out.println(k[0][0]);
		//System.out.println(k[0][1]);
		//System.out.println(k[1][0]);
		//System.out.println(k[1][1]);

		k = ProdottoMatriciQuadrate(weights,k,2);
		
		return CalcolaTraccia(k);
	}
	public static String Complementare(String sequenza) {
		String complementare = "";
		for(char c : sequenza.toCharArray()) {
			if (c == 'A') complementare += "T";
			if (c == 'T') complementare += "A";
			if (c == 'G') complementare += "C";
			if (c == 'C') complementare += "G";
			if (c == 'N') complementare += "N";
		    }		
		return complementare;
	}
	public static Boolean Contains(List<Tuple3<Double,String,String>> t,Double g) {
		for(Tuple3<Double,String,String> sequence : t) {
			if (sequence._1() == g ) {
				return true;
			}
		}
		return false;
	}
	public static <A, B> List<Tuple2<A, B>> zipJava8(List<A> as, List<B> bs) {
	    return IntStream.range(0, Math.min(as.size()-1, bs.size()-1))
	            .mapToObj(i -> new Tuple2<>(as.get(i), bs.get(i)))
	            .collect(Collectors.toList());
	}
	public static String reverseString(String str){  
	    StringBuilder sb=new StringBuilder(str);  
	    sb.reverse();  
	    return sb.toString();  
	}  
	
}
