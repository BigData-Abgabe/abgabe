package bigdata;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, TupleWritable, Text, TupleWritable> {

	@Override
	protected void reduce(Text key, Iterable<TupleWritable> values, Context context) throws IOException, InterruptedException {

		ArrayList<TupleWritable> leftVals = new ArrayList<TupleWritable>();
		ArrayList<TupleWritable> rightVals = new ArrayList<TupleWritable>();
		
		for (TupleWritable val : values) {
			TupleWritable store = new TupleWritable();
			for(int i = 1; i < val.getRow().size(); ++i)
				store.addString(val.getRow().get(i));
			if (val.getRow().get(0).equals("L"))
				leftVals.add(store);
			else
				rightVals.add(store);
		}
		
		
		for (TupleWritable left : leftVals) {
			for (TupleWritable right : rightVals) {

				TupleWritable outValue = new TupleWritable();
				
				for (int i = 0; i < left.getRow().size(); ++i) {
					outValue.addString(left.getRow().get(i));
				}
				for (int i = 0; i < right.getRow().size(); ++i) {
					outValue.addString(right.getRow().get(i));
				}
				context.write(key, outValue);
				
			}
		}
		
	}
}
