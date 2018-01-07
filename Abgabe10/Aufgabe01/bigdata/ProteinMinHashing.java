package bigdata;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ProteinMinHashing {

	public static final String a_nums = "minhash.a";
	public static final String b_nums = "minhash.b";

	public static void printUsageAndQuit() {
		System.out.println(
				"Please specifiy the two parameters <minhash.functions> <Pfad Shingleliste> <Matrix> <Ausgabepfad> and optionally which column to process");
		System.exit(0);
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 3)
			printUsageAndQuit();

		Configuration conf = new Configuration();
		GenericOptionsParser options = new GenericOptionsParser(conf, args);
		String remainingArguments[] = options.getRemainingArgs();

		int minhashFunctions = conf.getInt("minhash.functions", 10);

		// generate the random numbers a and b for the hash functions
		Random ran = new Random();
		StringBuilder a = new StringBuilder();
		StringBuilder b = new StringBuilder();

		for (int i = 0; i < minhashFunctions; i++) {
			a.append(ran.nextInt()).append(" ");
			b.append(ran.nextInt()).append(" ");
		}

		conf.set(a_nums, a.toString());
		conf.set(b_nums, b.toString());

		Job job = Job.getInstance(conf, "ProteinMinHashing");
		job.setJarByClass(ProteinMinHashing.class);

		job.setMapperClass(MinhashMapper.class);
		job.setReducerClass(MinhashReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(remainingArguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArguments[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
