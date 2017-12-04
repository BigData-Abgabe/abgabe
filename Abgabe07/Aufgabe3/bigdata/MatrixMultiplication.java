package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * David
 * Blatt 7, Aufgabe 3
 */
public class MatrixMultiplication {

    /**
     * Treiber Klasse, wie gewöhnlich
     * Einzige Besonderheit ist hier das Secondary Sort.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("path.A", args[0]); //Pfad Matrix A
        conf.set("path.B", args[1]); //Pfad Matrix B
        conf.set("rows.A", args[2]); //Anzahl Zeilen A
        conf.set("columns.B", args[3]); //Anzahl Spalten B
        Job job = Job.getInstance(conf, "Matrix Multiplication");

        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMultiplicationMapper.class);
        job.setReducerClass(MatrixMultiplicationReducer.class);

        //Secondary Sort, implementiere Partitioner, Group Comparator und Sort Comparator
        job.setPartitionerClass(MatrixMultiplicationPartitioner.class);
        //eigentlich überflüßig, da TupleWritable schon dafür sorgt
        //dass ein Tupel über das letzt Element sortiet wird
        job.setSortComparatorClass(MatrixMultiplicationSortComparator.class);
        //Gruppiere über den natural key
        job.setGroupingComparatorClass(MatrixMultiplicationGroupComparator.class);

        //RelationInputFormat, ließt je eine Zeile ein und übergibt den Pfad und ein Tupel von Attributen an den Mapper
        job.setInputFormatClass(RelationInputFormat.class);

        job.setMapOutputKeyClass(TupleWritable.class);
        job.setMapOutputValueClass(TupleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}