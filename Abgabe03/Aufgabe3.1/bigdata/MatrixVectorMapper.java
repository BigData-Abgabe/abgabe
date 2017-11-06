package bigdata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;


public class MatrixVectorMapper extends Mapper<Object, Text, Text, DoubleListWritable> {
	private StringTokenizer  vector;
	private Text k = new Text();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		ArrayList<Double> list = new ArrayList<>();
		vector = new StringTokenizer(context.getConfiguration().get("vector"));
		int amountTokens = vector.countTokens() -1;
		String valString = value.toString();
		if(valString.startsWith("#"))
			// This line is a comment
			return;
		StringTokenizer itr = new StringTokenizer(value.toString());
		// Return if line is missing cols
		if(!itr.hasMoreTokens())
			return;
		// First col as key
		k.set(itr.nextToken());
		// vector name is not needed
		vector.nextToken();
		// wrong format
		if(!itr.hasMoreTokens())
			return;
		// Other cols as value
		for(int i = 0; i< amountTokens;i++) {
			Double matrixvalue = Double.parseDouble(itr.nextToken());
			Double vectorvalue = Double.parseDouble(vector.nextToken());
			Double result = matrixvalue * vectorvalue;
			list.add(result);
		}
		DoubleListWritable v = new DoubleListWritable();
		v.setList(list);
		context.write(k, v);
	}
}



