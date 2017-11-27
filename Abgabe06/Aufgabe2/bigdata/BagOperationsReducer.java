package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BagOperationsReducer extends Reducer<TupleWritable, MapWritable, TupleWritable, NullWritable> {

	public enum Operation {
		UNION,INTERSECTION,DIFFERENCE
	}
	private Operation op;

	@Override
	protected void setup(Reducer<TupleWritable, MapWritable, TupleWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		String operation = configuration.get("operation");
		if (operation.equals("bagunion")) {
			op = Operation.UNION;
		} else if (operation.equals("bagintersection")) {
			op = Operation.INTERSECTION;
		} else if (operation.equals("bagdifference")) {
			op = Operation.DIFFERENCE;
		}
		//System.out.println(operation + " " + op);
	}

	@Override
	protected void reduce(TupleWritable key, Iterable<MapWritable> values,
			Reducer<TupleWritable, MapWritable, TupleWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		//Counts how many times a relation came from R or S
		int R = 0, S = 0;
		//Iterate through values
		for (MapWritable value : values) {
			R += ((IntWritable) value.get(new Text("R"))).get(); //Count how many times a relation came from R
			S += ((IntWritable) value.get(new Text("S"))).get(); //Do the same thing for S
		}	
		int count = 0;
		//Check the operation, union/intersection/difference
		switch (op) {
		case UNION:
			count = R + S; //Sum of the occurrences of S and R (Siehe Arbeitsblatt)
			break;
		case INTERSECTION:
			count = Math.min(R, S); //Minimum of the occurrences
			break;
		case DIFFERENCE:
			count = Math.max(0, R - S);//Maximum of the occuurences
			break;
		}

		//return the relation based on the occurrences 
		for (int i = 0; i < count; i++) {
			context.write(key, NullWritable.get());
		}
	}

}
