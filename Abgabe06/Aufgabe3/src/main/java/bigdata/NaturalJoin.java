package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NaturalJoin 
{
	public static void printUsageAndQuit() {
        System.out.println("Please specifiy the three parameters <Pfad Eingabedatei 1> <Pfad Eingabedatei 2> <Ausgabepfad> and optionally which column to process");
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2)
            printUsageAndQuit();
        Configuration conf = new Configuration();
        
        GenericOptionsParser options = new GenericOptionsParser(conf, args);
        String remainingArguments[] = options.getRemainingArgs();
		conf.set("L", remainingArguments[0]);

        Job job = Job.getInstance(conf, "NaturalJoin");
        job.setJarByClass(NaturalJoin.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TupleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TupleWritable.class);
        
		job.setInputFormatClass(RelationInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(remainingArguments[0]));
		FileInputFormat.addInputPath(job, new Path(remainingArguments[1]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArguments[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}