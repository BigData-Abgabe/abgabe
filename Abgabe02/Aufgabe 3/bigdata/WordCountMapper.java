package bigdata;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String keystring = itr.nextToken();
			keystring = keystring.replaceAll("[\\-\\+\\.\"\u201C\u201D\u201E\\[\\]\\(\\)\\^:,!,?]","");
			keystring = keystring.toLowerCase();
			word.set(keystring);
			context.write(word, one);
		}
	}
}