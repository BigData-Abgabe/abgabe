package bigdata;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.umd.cloud9.io.pair.PairOfStringInt;

public class ProteinLSHashingReducer extends Reducer<TupleWritable, PairOfStringInt, NullWritable, Text> {
	
	public final static int N_BUCKETS = 1000033;
	public final static int HASH_BASE = 21;
	
	public ArrayList<String>[] buckets = new ArrayList[N_BUCKETS];
    private StringBuilder outVal = new StringBuilder();
	
	public void reduce(TupleWritable key, Iterable<PairOfStringInt> vals, Context context) throws IOException, InterruptedException {
		for (int i = 0; i < N_BUCKETS; i++) {
            buckets[i] = null;
        }
        String oldProtein = "";
        int hash = 0;
        int exponent = 1;
        for (PairOfStringInt value : vals) {

            if (!oldProtein.equals(value.getLeftElement()) && !"".equals(oldProtein)) {
            	
                if (buckets[hash] == null) {
                    buckets[hash] = new ArrayList<String>();
                }
                
                buckets[hash].add(oldProtein);
                hash = 0;
                exponent = 1;
            }

            long x = 1;
            long base = HASH_BASE;
            int usedExponent = exponent;

            while (usedExponent > 0) {
                if (usedExponent % 2 == 1) {
                    x = (x * base) % N_BUCKETS;
                }
                
                base = (base * base) % N_BUCKETS;
                usedExponent /= 2;
            }
            
            long result = x % N_BUCKETS;

            result = (result * value.getRightElement()) % N_BUCKETS;

            result = (hash + result) % N_BUCKETS;
            hash = (int) result;
            exponent++;
            oldProtein = value.getLeftElement();
        }

        // Add last protein
        if (buckets[hash] == null)
            buckets[hash] = new ArrayList<String>();
        buckets[hash].add(oldProtein);
        
        
        for (ArrayList<String> bucket : buckets) {

        	// Check bucket only if it's non-empty
            if (bucket != null) {
            	
            	// Collect all pairs from the bucket
                for (int i = 0; i < bucket.size(); i++) {
                    for (int j = i + 1; j < bucket.size(); j++) {
                        outVal.setLength(0);
                        outVal.append(bucket.get(i)).append("\t").append(bucket.get(j));
                        
                        context.write(NullWritable.get(), new Text(outVal.toString()));
                    }
                }
            }
        }
	}
	
}
