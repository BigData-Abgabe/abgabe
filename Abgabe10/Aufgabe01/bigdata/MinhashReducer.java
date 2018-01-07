package bigdata;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.net.URI;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MinhashReducer extends Reducer<Text, IntWritable, Text, Text> {

	static int[] a;
	static int[] b;
	static List<Integer> shingleID = new ArrayList<>();
	static final int PRIME = 179999993;
	static Text minHash;

	@Override
	public void setup(Context context) throws IOException {
		// parse the random numbers of the driver from String to int
		String[] a_string = context.getConfiguration().get(ProteinMinHashing.a_nums).split(" ");
		String[] b_string = context.getConfiguration().get(ProteinMinHashing.b_nums).split(" ");

		a = new int[a_string.length];
		b = new int[b_string.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = Integer.parseInt(a_string[i]);
			b[i] = Integer.parseInt(b_string[i]);
		}

		// read the shingle file

		URI[] shingle_files = context.getCacheFiles();

		FileSystem fs = FileSystem.get(context.getConfiguration());
		BufferedReader bfr = new BufferedReader(new InputStreamReader(fs.open(new Path(shingle_files[0]))));
		String line;
		while ((line = bfr.readLine()) != null) {
			shingleID.add(Integer.parseInt(line));
		}
		bfr.close();
	}

	public void reduce(Text protein, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		Set<Integer> shingles = new TreeSet<Integer>();
		for (IntWritable value : values) {
			shingles.add(value.get());
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < a.length; i++) {
			sb.append(minHash(i, shingles)).append("\t");
		}
		minHash.set(sb.toString());
		context.write(protein, minHash);
	}

	public static int minHash(int i, Set<Integer> shingles) {

		for (int x = 0; x < shingleID.size(); x++) {
			int h = h(a[i], b[i], x, shingleID.size());
			if (shingles.contains(shingleID.get(h)))
				return x;
		}
		return Integer.MAX_VALUE;
	}

	public static int h(int a, int b, int x, int n) {
		int h = ((a * x + b) % PRIME) % n;
		if (h < 0)
			h += n;
		return h;

	}
}
