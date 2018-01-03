package bigdata;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TupleWritable implements WritableComparable<TupleWritable> {

    private List<String> list;

    /**
     * Constructor TupleWritable
     */
    public TupleWritable() {
    }

    /**
     * Constructor TupleWritable
     */
    public TupleWritable(List<String> list) {
        this.list = list;
    }

    /**
     * Gets the list
     *
     * @return the list
     */
    public List<String> getList() {
        return list;
    }

    /**
     * Sets the list
     *
     * @param list the list
     */
    public void setList(List<String> list) {
        this.list = list;
    }

    /**
     * Compares two tuples
     *
     * @param o tuple to compare with list
     * @return < 0 if list < o, 0 if list = o, > 0 if list > o
     */
    public int compareTo(TupleWritable o) {
        for (int i = 0; i < this.list.size(); i++) {
            int result = this.list.get(i).compareTo(o.getList().get(i));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    /**
     * Serialize the TupleWritable
     *
     * @param dataOutput the output stream
     * @throws IOException something goes wrong
     */
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

    /**
     * Deserialize the TupleWritable
     *
     * @param dataInput the input stream
     * @throws IOException something goes wrong
     */
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

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TupleWritable) {
            return compareTo((TupleWritable) obj) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.list.hashCode();
    }

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