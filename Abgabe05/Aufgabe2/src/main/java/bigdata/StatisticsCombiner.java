package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatisticsCombiner extends Reducer<NullWritable, MapWritable, NullWritable, MapWritable> {

    /**
     * Max-key
     */
    private static final Text MAX = new Text("Max");

    /**
     * Min-key
     */
    private static final Text MIN = new Text("Min");

    /**
     * Sum-key
     */
    private static final Text SUM = new Text("Sum");

    /**
     * Counter-key
     */
    private static final Text COUNTER = new Text("Counter");

    private DoubleWritable resultMin = new DoubleWritable();
    private DoubleWritable resultMax = new DoubleWritable();
    private DoubleWritable resultSum = new DoubleWritable();
    private DoubleWritable resultCounter = new DoubleWritable();


    private MapWritable map = new MapWritable();

    /**
     * Reduces individual columns to their statistic metrics.
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(NullWritable key, Iterable<MapWritable> values, Context context) throws IOException,
            InterruptedException {

        int counter = 0;
        double sum = 0;
        double max = Double.NEGATIVE_INFINITY;
        double min = Double.POSITIVE_INFINITY;
        for (MapWritable value : values) {
            sum += ((DoubleWritable) value.get(SUM)).get();
            double tmp = ((DoubleWritable) value.get(MIN)).get();
            if (tmp < min) {
                min = tmp;
            }
            tmp = ((DoubleWritable) value.get(MAX)).get();
            if (tmp > max) {
                max = tmp;
            }
            counter += (int) ((DoubleWritable) value.get(COUNTER)).get();
        }
        resultMin.set(min);
        map.put(MIN, resultMin);
        resultMax.set(max);
        map.put(MAX, resultMax);
        resultSum.set(sum);
        map.put(SUM, resultSum);
        resultCounter.set(counter);
        map.put(COUNTER, resultCounter);
        context.write(key, map);
    }
}