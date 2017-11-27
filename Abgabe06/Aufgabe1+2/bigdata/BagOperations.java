package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class BagOperations {

	public static void printUsageAndQuit() {
		System.out.println("Please specifiy the three parameters <Eingabepfad R> <Eingabepfad S> <Ausgabepfad>");
		System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2)
			printUsageAndQuit();
		Configuration conf = new Configuration();
		GenericOptionsParser options = new GenericOptionsParser(conf, args);
		String[] remainingArgs = options.getRemainingArgs();
		conf.set("PathOfSetR", remainingArgs[0]);
		Job job = Job.getInstance(conf, "BagOperations");
		job.setJarByClass(BagOperations.class);
		job.setMapperClass(BagOperationsMapper.class);
		job.setReducerClass(BagOperationsReducer.class);
		job.setMapOutputKeyClass(TupleWritable.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setInputFormatClass(RelationInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		FileInputFormat.addInputPath(job, new Path(remainingArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);


	}

}
