package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import tl.lin.data.pair.PairOfIntString;
import tl.lin.data.pair.PairOfInts;

public class ProteinJaccardSimilarity {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        GenericOptionsParser options = new GenericOptionsParser(conf, args);
        String remainingArguments[] = options.getRemainingArgs();

        
        Job job = Job.getInstance(conf, ProteinJaccardSimilarity.class.getName());
        job.setJarByClass(ProteinJaccardSimilarity.class);
        job.setMapperClass(PJSMapper.class);
        job.setReducerClass(PJSReducer.class);
        
        job.setMapOutputKeyClass(PairOfInts.class);
        job.setMapOutputValueClass(PairOfIntString.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        // it might be useful to set reduce-tasks to 0 to see mapper output
        job.setNumReduceTasks(conf.getInt("jaccard.reducers", 1));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, new Path(remainingArguments[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArguments[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
