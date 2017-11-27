package bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class TupleWritable implements WritableComparable<TupleWritable> {
    private List<String> column;

    public TupleWritable(){
        column = new ArrayList<String>();
    }

    //Adds a text as tuple into our list
    public TupleWritable(Text text){
        column = new ArrayList<String>();
        String s = text.toString();
        String [] line = s.split("\t");
        Collections.addAll(column, line);
    }

    //compare two tuples, Siehe Arbeitsblatt
    public int compareTo(TupleWritable t) {
        if (t.column.equals(this.column))
            return 0;
        for (int i = 0; i < Math.min(t.column.size(), this.column.size()); i++) {
            if (i == Math.min(t.column.size(), this.column.size())) {
                return t.column.size() < this.column.size() ? -1 : 1;
            }
            if (t.column.get(i).equals(this.column.get(i))) {
                continue;
            }
            return this.column.get(i).compareTo(t.column.get(i));
        }
        return 0;
    }

    //Serializes & deserializes
    
    public void write(DataOutput dataOutput) throws IOException {
       Text t = new Text(this.toString()); //Overwritten toString, see below
       t.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        Text t = new Text();
        t.readFields(dataInput);
        String [] sar = t.toString().split("\t"); //split at tabs
        this.column.clear(); //!!! otherwise wrong result, should be empty every time we read data
        for(String s : sar){
            this.column.add(s); //Add attributes
        }
    }
    
    //Two tuples are equals, if the attributes of the tupel are equal
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TupleWritable)) {
            return false;
        }
        TupleWritable t = (TupleWritable) o;
        return this.column.equals(t.column);
    }

    @Override
    public int hashCode() {
        return column.hashCode();
    }

    //For the sole purpose of displaying a relation, adds a tab (\t) after every attribute of a relation (ignores the last Element)
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < column.size()-1; i++){
            sb.append(column.get(i)).append("\t");
        }
        sb.append(column.get(column.size()-1));
        return sb.toString();
    }
    
    public int getAsInt(int index) {
    	return Integer.parseInt(column.get(index));
    }

    public List<String> getRow(){
        return this.column;
    }

    public void addString(String s){
        this.column.add(s);
    }

    public void clear(){
        this.column.clear();
    }
}
