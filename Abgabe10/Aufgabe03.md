# Aufgabe 3

``` Java
public static void main(String ... args) {
    SparkConf conf = new SparkConf().setAppName("SimpleWordCount");
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    sc.textFile(args[0])
            // (a) Wendet die übergebene Funktion auf jedes Element der gelesenen Datei an und erzeugt eine flache JavaRDD<String>, die zurückgegeben wird
            .flatMap(l -> Arrays.asList(l.split(" ")).iterator())
            // (b) Wendet die übergebene Funktion, die eine Pair erzeugen muss, auf jedes Element des Eingabe-RDD an und gibt ein JavaPairRDD<String, Integer> zurück, 
            //     das den Inhalt der erzeugten Tuple2<String, Integer> enthält
            .mapToPair(w -> new Tuple2<String, Integer>(w, 1))
            // (c) Wendet die übergebene Funktion (wie ein Combiner bei M/R) in Paaren auf alle values im Eingabe-RDD an, die den gleichen key haben. Es wird wieder ein JavaPairRDD<String, Integer> zurückgegeben
            .reduceByKey((x, y) -> x + y)
            // write word counts
            .saveAsTextFile(args[1]);
}
```
