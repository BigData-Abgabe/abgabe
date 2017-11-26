package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;

public class TupleWritable implements WritableComparable<TupleWritable> {

	ArrayList<String> tuple = new ArrayList<String>();
	
	public TupleWritable() {
	}
	
	public TupleWritable(String s) {
			
		setTuple(s);
	}

	public ArrayList<String> getTupel() {
		return tuple;
	}
	
	public void setTuple(ArrayList<String> words) {
		this.tuple = words;
	}
	
	public void setTuple(String s) {
		tuple = new ArrayList<String>();
		StringTokenizer itr = new StringTokenizer(s);
		while (itr.hasMoreTokens()) {
			tuple.add(itr.nextToken());
			}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		String words = in.readUTF();
		StringTokenizer itr = new StringTokenizer(words);
		while (itr.hasMoreTokens()) {
			tuple.add(itr.nextToken());
			}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		String output="";
		for (String word: this.tuple){
			output+= word;
		}
		out.writeUTF(output);
	}
	
	@Override
	public int compareTo(TupleWritable other) {
	
	ArrayList<String> words= other.getTupel();
	int index = (this.tuple.size() > words.size())? this.tuple.size():words.size() ;
	int cmp = 0;
	
	if (this.equals(other))
		return 0;
	
	for (int i=0; i < index-1; i++){
		
			cmp = this.tuple.get(i).compareTo(words.get(i));
			if (cmp != 0) {  // if both words are not equal give order, else test the next attribute
				return cmp;
			}
		}
	return (this.tuple.size() > words.size())? -1:1;
	}
	

	public boolean equals(Object o){
		
		if ( o == null )
			return false;

	    if ( o == this )
	    	return true;
	     
	    if ( ! o.getClass().equals(getClass()) )
	    	return false; 
	      
		TupleWritable other= (TupleWritable) o;
		ArrayList<String> words= other.getTupel();
		
		if (this.tuple.size() == words.size())
			return false;
		
		Iterator<String> itrA=tuple.iterator();
		Iterator<String> itrB=words.iterator();
		
		while(itrA.hasNext()){
			if (!(itrA.next().equals(itrB.next())))
				return false;
			
		}
		
		return true;
	}
	
	@Override
	public int hashCode(){
		return this.tuple.hashCode();
	}
	
}