package bigdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class SimpleStatisticsTest {

	@Test
	public void testMapper() throws Exception {
		new MapDriver<Object, Text, NullWritable, FloatWritable>()
			.withMapper(new StatisticsMapper())
			.withInput(NullWritable.get(), new Text("#Name	v1	v2"))
			.withInput(NullWritable.get(), new Text("Jenny	2.0	2.7"))
			.withInput(NullWritable.get(), new Text("Sebastian	9.3	0.0"))
			.withInput(NullWritable.get(), new Text("Thomas	0.7	4.2"))
			.withOutput(NullWritable.get(), new FloatWritable((float) 2.0))
			.withOutput(NullWritable.get(), new FloatWritable((float) 9.3))
			.withOutput(NullWritable.get(), new FloatWritable((float) 0.7))
			.runTest();
	}
	
	@Test
	public void testReducer() throws Exception {
		List<FloatWritable> values = new ArrayList<FloatWritable>();
		values.add(new FloatWritable((float) 2.0));
		values.add(new FloatWritable((float) 9.3));
		values.add(new FloatWritable((float) 0.7));
		
		new ReduceDriver<NullWritable, FloatWritable, Text, FloatWritable>()
			.withReducer(new StatisticsReducer())
			.withInput(NullWritable.get(), values)
			.withOutput(new Text("Min"), new FloatWritable((float) 0.7))
			.withOutput(new Text("Max"), new FloatWritable((float) 9.3))
			.withOutput(new Text("Sum"), new FloatWritable((float) 12.0))
			.withOutput(new Text("Avg"), new FloatWritable((float) 4.0))
			.runTest();
	}

	@Test
	public void testMapReduce() throws Exception {
		new MapReduceDriver<Object, Text, NullWritable, FloatWritable, Text, FloatWritable>()
			.withMapper(new StatisticsMapper())
			.withInput(NullWritable.get(), new Text("#Name	v1	v2"))
			.withInput(NullWritable.get(), new Text("Jenny	2.0	2.7"))
			.withInput(NullWritable.get(), new Text("Sebastian	9.3	0.0"))
			.withInput(NullWritable.get(), new Text("Thomas	0.7	4.2"))
			.withReducer(new StatisticsReducer())
			.withOutput(new Text("Min"), new FloatWritable((float) 0.7))
			.withOutput(new Text("Max"), new FloatWritable((float) 9.3))
			.withOutput(new Text("Sum"), new FloatWritable((float) 12.0))
			.withOutput(new Text("Avg"), new FloatWritable((float) 4.0))
			.runTest();
	}

	
}
