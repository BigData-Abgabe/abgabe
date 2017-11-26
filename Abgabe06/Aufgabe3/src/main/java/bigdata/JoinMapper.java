package bigdata;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<Text, TupleWritable, Text, TupleWritable> {
	
	Text leftFilename = new Text();
	IntWritable left = new IntWritable(), right = new IntWritable();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		leftFilename.set(configuration.get("L"));
		left.set(configuration.getInt("joincol.left", 0));
		right.set(configuration.getInt("joincol.right", 0));
	}
	
	@Override
	protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
		
		boolean isLeft = key.toString().contains(leftFilename.toString());
		
		int keyIndex = isLeft ? left.get() : right.get();
		List<String> vals = value.getRow();
		Text joinKey = new Text(vals.get(keyIndex));
		TupleWritable outValue = new TupleWritable();
		outValue.addString(isLeft ? "L" : "R");
		
		for (int i = 0; i < vals.size(); ++i) {
			if (i == keyIndex)
				continue;
			outValue.addString(vals.get(i));
		}
		
		context.write(joinKey, outValue);
		
		
	}

}
