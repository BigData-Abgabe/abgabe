package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class TupleWritable implements WritableComparable<TupleWritable> {

	private List<String> tuple;
	
	public TupleWritable() {
		tuple = new ArrayList<String>();
	}
	
	public TupleWritable(String s) {
		setTuple(s);
	}
	
	// generates tuple List via Text element
	public TupleWritable(Text t) {
		setTuple(t.toString());
	}
	

	public List<String> getTupel() {
		return tuple;
	}
	
	public void setTuple(ArrayList<String> words) {
		this.tuple = words;
	}
	
	//setter for Tuple
	public void setTuple(String s) {
		tuple = new ArrayList<String>();
		StringTokenizer itr = new StringTokenizer(s);
		while (itr.hasMoreTokens()) {
			tuple.add(itr.nextToken());
			}
	}
	
	public void setTuple(Text t) {
		setTuple(t.toString());
	}
	
	 //Serializes & deserializes
	@Override
	public void readFields(DataInput in) throws IOException {
        Text t = new Text();
        t.readFields(in);
        String [] sar = t.toString().split("\t"); //split at tabs
        this.tuple.clear(); //!!! otherwise wrong result, should be empty every time we read data
        for(String s : sar){
            this.tuple.add(s); //Add attributes
        }
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		Text t = new Text (this.toString()); //Overwritten toString, see below
		t.write(out);
	}
	
	 //compare two tuples, Siehe Arbeitsblatt
	@Override
	public int compareTo(TupleWritable other) {
	
    if (other.tuple.equals(this.tuple))
        return 0;
    for (int i = 0; i < Math.min(other.tuple.size(), this.tuple.size()); i++) {
        if (i == Math.min(other.tuple.size(), this.tuple.size())) {
            return other.tuple.size() < this.tuple.size() ? -1 : 1;
        }
        if (other.tuple.get(i).equals(this.tuple.get(i))) {
            continue;
        }
        return this.tuple.get(i).compareTo(other.tuple.get(i));
    }
    return 0;
}
	
	//Two tuples are equals, if the attributes of the tupel are equal
	public boolean equals(Object o){
		
		if ( o == null )
			return false;

	    if ( o == this )
	    	return true;
	     
	    if ( ! o.getClass().equals(getClass()) )
	    	return false; 
	      
		TupleWritable other= (TupleWritable) o;
		
		return this.toString().equals(other.toString());
			

	}

	  public void addString(String s){
	        this.tuple.add(s);
	   }
	
	@Override
	public int hashCode(){
		return this.tuple.hashCode();
	}
	
	//For the sole purpose of displaying a relation, adds a tab (\t) after every attribute of a relation (ignores the last Element) and to compare tuples
	@Override
	public String toString(){
		
		StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tuple.size()-1; i++){
            sb.append(tuple.get(i)).append("\t");
        }
        sb.append(tuple.get(tuple.size()-1));
        return sb.toString();
	}
	
    public int getAsInt(int index) {
    	return Integer.parseInt(tuple.get(index));
    }
	
    public List<String> getRow(){
        return this.tuple;
    }
	
}