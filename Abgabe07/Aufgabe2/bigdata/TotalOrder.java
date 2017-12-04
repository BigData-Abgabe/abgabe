package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;

public class TotalOrder {

	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path partitionPath = new Path(args[1]+"-part");

		Job job = Job.getInstance(conf, "TotalOrder");
		
		job.setJarByClass(TotalOrder.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		//initialize TotalOrderPartitioner
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionPath);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//use total order partitioner
		job.setPartitionerClass(TotalOrderPartitioner.class);	
		
		// for this task standard mapper and reducer can be applied 
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//set number of Reducers
		job.setNumReduceTasks(10);
		

		//set Sampler and write sample sheet - the jars were generated using one of the following split sampler
		
		//set random sampler (splits per reducer: ranging from 5200 -10500)
		//InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler(0.01, 1000);
		
		//set split sampler with 1000 (splits per reducer: ranging from 6600 -7500)
		//InputSampler.Sampler<Text, Text> sampler = new InputSampler.SplitSampler(1000);
		
		//set split sampler with 3000 (splits per reducer: ranging from 1500 -13000)
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.SplitSampler(3000);
		InputSampler.writePartitionFile(job, sampler);
		
		/* Conclusion
		 * 
		 * Split sampler 1000 improved the workload, to be more balanced compared to random split. Nevertheless, setting 3000 unbalances the  
		 * workload more drastically than the random split version. Therefore in this example SplitSampler 1000 is preferred.
		 * 
		 */
		
		FileOutputFormat.setOutputPath(job,outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

}
