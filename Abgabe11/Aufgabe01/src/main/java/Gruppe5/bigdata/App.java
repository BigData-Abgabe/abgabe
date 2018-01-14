package Gruppe5.bigdata;

/**
 * David B.
 *
 */
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import java.util.Arrays;

public class App {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("App").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.textFile(args[0]) //read textfile
		.flatMap(l->Arrays.asList(l.split("\n")).iterator()) //split at every row
		.zip(sc.textFile(args[1]).flatMap(l->Arrays.asList(l.split("\n")).iterator())) //create an RDD-pair, containing rows of matrix A and of matrix B
		.map(x-> { //for every row
			String result = "";
			for(int i = 0;i<x._1.split("\t").length;i++) { //loop through elements of a row
				result += (Double.parseDouble(x._1.split("\t")[i]) + Double.parseDouble(x._2.split("\t")[i]) + "\t"); //add every element from a row
			}
			return result.replaceAll("\\s+$", ""); //removes whitespace at the end
		}).saveAsTextFile(args[2]);
	}
}
