package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopTen {
	
	public static final int TOP_NUM = 10;

    public static void printUsageAndQuit() {
        System.out.println("Please specifiy the two parameters <Pfad Eingabedatei> <Ausgabepfad>");
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2)
            printUsageAndQuit();
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TopTen");
        job.setJarByClass(TopTen.class);
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(SortedMapWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
