package bigdata;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class TupleWritable implements WritableComparable<TupleWritable> {

    private List<String> list;

    public TupleWritable() {
    }

    public TupleWritable(List<String> list) {
        this.list = list;
    }

    public List<String> getList() {
        return list;
    }

    public void setList(List<String> list) {
        this.list = list;
    }

    //comparing, needed for shuffle/sort phase
    public int compareTo(TupleWritable o) {
        for (int i = 0; i < this.list.size(); i++) {
            int result = this.list.get(i).compareTo(o.getList().get(i));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    //write size of the list (amount of attributes in a tuple)
    //afterwards write all elements in the list
    public void write(DataOutput dataOutput) throws IOException {
        if (this.list != null) {
            dataOutput.writeInt(this.list.size());
            for (String entry : this.list) {
                dataOutput.writeUTF(entry);
            }
        } else {
            dataOutput.writeInt(0);
        }
    }

   //read the size of the list, afterwards read size amount of attributes and add them to the list
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        if (size != 0) {
            this.list = new ArrayList<String>();
            for (int i = 0; i < size; i++) {
                String input = dataInput.readUTF();
                list.add(input);
            }
        }
    }

    //check if two tuples are equal
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TupleWritable) {
            return compareTo((TupleWritable) obj) == 0;
        }
        return false;
    }

    //hash
    @Override
    public int hashCode() {
        return this.list.hashCode();
    }

    //Displaying a tuple, skips last Element
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String entry : this.list) {
            sb.append(entry).append("\t");
        }
        sb.setLength(Math.max(sb.length() - 1, 0));
        return sb.toString();
    }
}