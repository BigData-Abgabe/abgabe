package bigdata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixVector {

	public static void printUsageAndQuit() {
		System.out.println("Please specifiy the three parameters <Pfad Eingabematrix> <EingabeVector> <AusgabePfad>");
		System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2)
			printUsageAndQuit();
		Configuration conf = new Configuration();

		try {
			BufferedReader b = new BufferedReader(new FileReader(args[1]));
			conf.set("vector",b.readLine());
			b.close();

		} catch (IOException e) {

			e.printStackTrace();

		} 
	Job job = Job.getInstance(conf, "MatrixVector");
	job.setJarByClass(MatrixVector.class);
	job.setMapperClass(MatrixVectorMapper.class);
	job.setReducerClass(MatrixVectorReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleListWritable.class);
	KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[2]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);

}

}
