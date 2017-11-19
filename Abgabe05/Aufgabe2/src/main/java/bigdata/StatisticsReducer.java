package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatisticsReducer extends Reducer<NullWritable, MapWritable, Text, FloatWritable> {

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

    /**
     * Avg-key
     */
    private static final Text AVG = new Text("Avg");

    private FloatWritable result = new FloatWritable();

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
        float sum = 0;
        float max = Float.NEGATIVE_INFINITY;
        float min = Float.POSITIVE_INFINITY;
        for (MapWritable value : values) {
            sum += ((FloatWritable) value.get(SUM)).get();
            float tmp = ((FloatWritable) value.get(MIN)).get();
            if (tmp < min) {
                min = tmp;
            }
            tmp = ((FloatWritable) value.get(MAX)).get();
            if (tmp > max) {
                max = tmp;
            }
            counter += (int) ((FloatWritable) value.get(COUNTER)).get();
        }
        float avg = sum / counter;
        result.set(min);
        context.write(MIN, result);
        result.set(max);
        context.write(MAX, result);
        result.set(sum);
        context.write(SUM, result);
        result.set(avg);
        context.write(AVG, result);
    }

}
