package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MatMult {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("MatMult");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<int[]> MatA = readMatrix(args[0], sc);
		JavaRDD<int[]> MatB = readMatrix(args[1], sc);
		
		// Reduce des ersten Pass vom 2-Pass M/R Algorithmus
		JavaPairRDD<Integer, int[]> MatAj = MatA.mapToPair(v -> new Tuple2<>(v[1], new int[] {v[0], v[2]}));
		JavaPairRDD<Integer, int[]> MatBj = MatB.mapToPair(v -> new Tuple2<>(v[0], new int[] {v[1], v[2]}));
		JavaPairRDD<String, Integer> valProducts = MatAj.join(MatBj)
				.values()
				.mapToPair(ab -> new Tuple2<>(ab._1[0] + "\t" + ab._2[0], ab._1[1] * ab._2[1]));
		
		// Reduce des zweiten Pass vom 2-Pass M/R Algorithmus
		JavaRDD<String> MatC = valProducts.reduceByKey((x, y) -> x + y)
				.filter(kv -> kv._2 != 0)
				.map(kv -> kv._1() + "\t" + kv._2)
				.sortBy(f -> f, true, 1);
		
		MatC.saveAsTextFile(args[2]);
		
		sc.close();		
	}
	
	private static JavaRDD<int[]> readMatrix(String file, JavaSparkContext sc) {
		return sc.textFile(file)
				.filter(l -> !l.startsWith("#")) // Kommentarzeilen ignorieren
				.map(l -> { // Zeilen in Tupel zerlegen (bzw. hier int-Arrays)  
					String col[] = l.split("\t");
					return new int[] {Integer.parseInt(col[0]), 
									  Integer.parseInt(col[1]), 
									  Integer.parseInt(col[2])};
				});
	}

}
