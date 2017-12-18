package bigdata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import tl.lin.data.pair.PairOfIntString;
import tl.lin.data.pair.PairOfInts;

public class PJSReducer extends Reducer<PairOfInts, PairOfIntString, Text, DoubleWritable> {
	
	private Text outK = new Text();
	private DoubleWritable outV = new DoubleWritable();
	
	private double jaccardThreshold;
	private int jaccardGroups;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
        //read values from console, threshold by default 0.8
		jaccardThreshold = context.getConfiguration().getDouble("jaccard.threshold", 0.8);
		System.out.println("jaccard.threshold = " + jaccardThreshold);
        //groups by default 10
		jaccardGroups = context.getConfiguration().getInt("jaccard.groups", 10);
		System.out.println("jaccard.groups = " + jaccardGroups);
	}


	@Override
	protected void reduce(PairOfInts uv, Iterable<PairOfIntString> values, Context context) 
			throws IOException, InterruptedException {
		int u = uv.getLeftElement();
		int v = uv.getRightElement();
		
		Map<String, Set<Integer>> proteinToShingles = new HashMap<>();
        //loop through all elements of the list in values
		for(PairOfIntString shingleAndProtein: values) { // for every pair (shingle,proteine)
			int shingleCode = shingleAndProtein.getLeftElement(); //shingle
			String protein = shingleAndProtein.getRightElement(); //proteine
			Set<Integer> shingles = proteinToShingles.get(protein); //return the set of shingles corresponding to a proteine
			if (shingles == null) {
				shingles = new TreeSet<Integer>();
				proteinToShingles.put(protein, shingles); // add proteine and the corresponding set of shingles to the map
			}
			shingles.add(shingleCode); //add shinglecode to the set (only unique elements)
		}
		//tell compiler the upcoming actions are legal....
		@SuppressWarnings("unchecked")
        //convert Map of proteine and shingle sets to  an entry array, so we can loop through it easier
        //compare two groups with eachother
        //within a group, compare proteines if they are 'similar"
		Entry<String,Set<Integer>>[] l = proteinToShingles.entrySet().toArray(new Entry[0]);
		for (int i = 0; i < l.length; i++) { //Wie in der vorlesung..
			int gi = PJSMapper.proteinToGroup(l[i].getKey(), jaccardGroups); //gruppe i
			for (int j = i + 1; j < l.length; j++) { //loop through entryarray
				int gj = PJSMapper.proteinToGroup(l[j].getKey(), jaccardGroups);//gruppe j
				// Keep in mind: u <= v
                //Wie in der vorlesung..
				if (gi != gj || (gi /* == gj */ == u && u == (v + 1) % jaccardGroups) 
						     || (gi /* == gj */ == v && v == (u + 1) % jaccardGroups)) {
					double s = jaccard(l[i].getValue(), l[j].getValue());

					if (s >= jaccardThreshold) { //check if bigger than jaccard value, only emit if it is bigger
						if (l[i].getKey().compareTo(l[j].getKey()) < 0) { // sort things
							outK.set(l[i].getKey() + "\t" + l[j].getKey());
						} else {
							outK.set(l[j].getKey() + "\t" + l[i].getKey());
						}
						outV.set(s);
						context.write(outK, outV);
					}
					
				}
			}
		}
		
	}


    //jaccardvalue
	private static double jaccard(Set<Integer> a, Set<Integer> b) {
		int intersectionCardinality = 0;
		for (Integer eInA : a) {
			if (b.contains(eInA)) {
				intersectionCardinality++;
			}
		}
		int unionCardinality = a.size() + b.size() - intersectionCardinality;
		return (double) intersectionCardinality / (double) unionCardinality;
	}
}
