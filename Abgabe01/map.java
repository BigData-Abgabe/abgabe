packagede.unimainz.informatik.bio.hadoop;importorg.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;importjava.util.StringTokenizer;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one= newIntWritable(1);
	private Text word= newText();
	public void map(Objectkey, Text value, Contex tcontext)
			throwsIOException, InterruptedException{
		
		String Tokenizer itr= new StringTokenizer(value.toString());
		while(itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, one);
		}
	}
}