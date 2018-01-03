package bigdata;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.io.pair.PairOfStringInt;

public class ProteinLSHashingMapper extends Mapper<Text, TupleWritable, TupleWritable, PairOfStringInt> {

	IntWritable bandwidth = new IntWritable();
    private ArrayList<String> keyList = new ArrayList<String>(3);
    private TupleWritable outKey = new TupleWritable();
    private PairOfStringInt outVal = new PairOfStringInt();
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        bandwidth.set(conf.getInt("lsh.bandwidth", 5));
    }
	
	@Override
	protected void map(Text key, TupleWritable val, Context context) throws IOException, InterruptedException {
		// Ignore comment lines
		if (key.toString().startsWith("#"))
			return;
		
		int signatureLength = val.getList().size() - 1;
        int bandNumber = signatureLength / bandwidth.get();
        int counter = 1;
        
        for (int i = 0; i < bandNumber; ++i) {
            for (int j = 0; j < bandwidth.get(); ++j) {
                keyList.clear();
                keyList.add(Integer.toString(i));
                keyList.add(val.getList().get(0));
                keyList.add(Integer.toString(counter));
                outVal.set(val.getList().get(0), Integer.parseInt(val.getList().get(counter)));
                outKey.setList(keyList);
                ++counter;
                context.write(outKey, outVal);
            }
            
            // If the length of the signature cannot be divided by the bandwidth the first rest bands get one column
            // more.
            if (i < signatureLength % bandwidth.get()) {
                keyList.clear();
                keyList.add(Integer.toString(i));
                keyList.add(val.getList().get(0));
                keyList.add(Integer.toString(counter));
                outVal.set(val.getList().get(0), Integer.parseInt(val.getList().get(counter)));
                outKey.setList(keyList);
                ++counter;
                context.write(outKey, outVal);
            }
        }
		
	}
	
}
