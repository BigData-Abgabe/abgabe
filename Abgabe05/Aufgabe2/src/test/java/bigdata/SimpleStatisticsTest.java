package bigdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class SimpleStatisticsTest {

    
    private static final Text MAX = new Text("Max");
    private static final Text MIN = new Text("Min");
    private static final Text SUM = new Text("Sum");
    private static final Text COUNTER = new Text("Counter");
    private static final FloatWritable ONE = new FloatWritable(1);
	@Test
	public void testMapper() throws Exception {
		MapWritable out1 = new MapWritable();
        out1.put(MIN, new FloatWritable((float) 2.0));
        out1.put(MAX, new FloatWritable((float) 2.0));
        out1.put(SUM, new FloatWritable((float) 2.0));
        out1.put(COUNTER, ONE);
		MapWritable out2 = new MapWritable();
        out2.put(MIN, new FloatWritable((float) 9.3));
        out2.put(MAX, new FloatWritable((float) 9.3));
        out2.put(SUM, new FloatWritable((float) 9.3));
        out2.put(COUNTER, ONE);
		MapWritable out3 = new MapWritable();
        out3.put(MIN, new FloatWritable((float) 0.7));
        out3.put(MAX, new FloatWritable((float) 0.7));
        out3.put(SUM, new FloatWritable((float) 0.7));
        out3.put(COUNTER, ONE);
		new MapDriver<Object, Text, NullWritable, MapWritable>()
			.withMapper(new StatisticsMapper())
			.withInput(NullWritable.get(), new Text("#Name	v1	v2"))
			.withInput(NullWritable.get(), new Text("Jenny	2.0	2.7"))
			.withInput(NullWritable.get(), new Text("Sebastian	9.3	0.0"))
			.withInput(NullWritable.get(), new Text("Thomas	0.7	4.2"))
			.withOutput(NullWritable.get(), out1)
			.withOutput(NullWritable.get(), out2)
			.withOutput(NullWritable.get(), out3)
			.runTest();
	}
	
	@Test
	public void testReducer() throws Exception {
		List<MapWritable> vals = new ArrayList<MapWritable>();
		MapWritable out1 = new MapWritable();
        out1.put(MIN, new FloatWritable((float) 2.0));
        out1.put(MAX, new FloatWritable((float) 2.0));
        out1.put(SUM, new FloatWritable((float) 2.0));
        out1.put(COUNTER, ONE);
		MapWritable out2 = new MapWritable();
        out2.put(MIN, new FloatWritable((float) 9.3));
        out2.put(MAX, new FloatWritable((float) 9.3));
        out2.put(SUM, new FloatWritable((float) 9.3));
        out2.put(COUNTER, ONE);
		MapWritable out3 = new MapWritable();
        out3.put(MIN, new FloatWritable((float) 0.7));
        out3.put(MAX, new FloatWritable((float) 0.7));
        out3.put(SUM, new FloatWritable((float) 0.7));
        out3.put(COUNTER, ONE);
        vals.add(out1);
        vals.add(out2);
        vals.add(out3);
		
		new ReduceDriver<NullWritable, MapWritable, Text, FloatWritable>()
			.withReducer(new StatisticsReducer())
			.withInput(NullWritable.get(), vals)
			.withOutput(new Text("Min"), new FloatWritable((float) 0.7))
			.withOutput(new Text("Max"), new FloatWritable((float) 9.3))
			.withOutput(new Text("Sum"), new FloatWritable((float) 12.0))
			.withOutput(new Text("Avg"), new FloatWritable((float) 4.0))
			.runTest();
	}

	@Test
	public void testMapReduce() throws Exception {
		new MapReduceDriver<Object, Text, NullWritable, MapWritable, Text, FloatWritable>()
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
