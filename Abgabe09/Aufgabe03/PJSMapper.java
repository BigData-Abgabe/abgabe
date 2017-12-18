package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import tl.lin.data.pair.PairOfIntString;
import tl.lin.data.pair.PairOfInts;

public class PJSMapper extends Mapper<Text, Text, PairOfInts, PairOfIntString> {

	private PairOfInts groupId = new PairOfInts();
	private PairOfIntString shingleCodeProteinId = new PairOfIntString();
	private int jaccardGroups;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
        //amount of groups
		jaccardGroups = context.getConfiguration().getInt("jaccard.groups", 10);
        //should be atleast 2
		if (jaccardGroups < 2) {
			throw new IllegalArgumentException("must have more than 2 groups");
		}
		//System.out.println("jaccard.groups = " + jaccardGroups);
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		int shingle = Integer.parseInt(key.toString());
        //substring from beginning to the first tab
		String protein = value.toString().substring(0, value.find("\t"));
        //create pair of (int,string) = (shingle,protein)
		shingleCodeProteinId.set(shingle, protein);
        //See static method at the bottom, assigns id for every proteine to a jaccard-group
		int u = proteinToGroup(protein, jaccardGroups);
        //(Siehe Vorlesung -> Gruppenbasierte Similarity Join)
		for (int v = 0; v < jaccardGroups; v++) {
			if (u == v) {
				continue;
			}
			if (u < v) {// make sure that the smaller value is on the left - easier than implementing this as set
				groupId.set(u, v); //{i,j}
			} else {
				groupId.set(v, u); // {i,j}
			}
			context.write(groupId, shingleCodeProteinId); //emit({i,j},(shingle,proteine))
		}
	}

	// Assigns a Protein-ID to a Jaccard-group
	static int proteinToGroup(String protein, int jaccardGroups) {
		int u = protein.hashCode() % jaccardGroups;
		if (u < 0) { // we want the (positive) modulus
			u += jaccardGroups;
		}
		return u;
	}

}
