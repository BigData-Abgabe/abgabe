package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SimpleStatistics {

    public static void printUsageAndQuit() {
        System.out.println("Please specifiy the two parameters <Pfad Eingabedatei> <Ausgabepfad>");
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2)
            printUsageAndQuit();
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Simple Statistics");
        job.setJarByClass(SimpleStatistics.class);
        job.setMapperClass(StatisticsMapper.class);
        job.setReducerClass(StatisticsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
