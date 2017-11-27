package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BagOperationsMapper extends Mapper<Text, TupleWritable, TupleWritable, MapWritable> {

	private String PathOfSetR;
	static IntWritable one = new IntWritable(1), zero = new IntWritable(0);
	private MapWritable map = new MapWritable();

	@Override
	protected void setup(Mapper<Text, TupleWritable, TupleWritable, MapWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		//We need this later to determine whether the relation came from R or not (then it is in S)
		PathOfSetR = configuration.get("PathOfSetR");
	}

	@Override
	protected void map(Text key, TupleWritable value,
			Mapper<Text, TupleWritable, TupleWritable, MapWritable>.Context context)
			throws IOException, InterruptedException {
		map.clear();
		//Check if the relation came from R
		if (key.toString().contains(PathOfSetR)) {
			map.put(new Text("R"), one);
			map.put(new Text("S"), zero);
		} else { //Relation came form S
			map.put(new Text("R"), zero);
			map.put(new Text("S"), one);
		}

		context.write(value, map);
	}

}