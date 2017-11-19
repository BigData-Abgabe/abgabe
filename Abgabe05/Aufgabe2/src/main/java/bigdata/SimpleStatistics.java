package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SimpleStatistics {

    public static void printUsageAndQuit() {
        System.out.println("Please specifiy the two parameters <Pfad Eingabedatei> <Ausgabepfad> and optionally which column to process");
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2)
            printUsageAndQuit();
        Configuration conf = new Configuration();
        
        GenericOptionsParser options = new GenericOptionsParser(conf, args);
        String remainingArguments[] = options.getRemainingArgs();

        Job job = Job.getInstance(conf, "Simple Statistics v2");
        job.setJarByClass(SimpleStatistics.class);
        job.setMapperClass(StatisticsMapper.class);
        job.setReducerClass(StatisticsReducer.class);
        
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(remainingArguments[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArguments[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
