package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Writable;

public class DoubleListWritable implements Writable {
	private List<Double> list;
	
	public DoubleListWritable() {
		this.list = new ArrayList<Double>();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.list.clear();
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			this.list.add(in.readDouble());
		}

	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.list.size());
		for (int i = 0; i<this.list.size();i++) {
			out.writeDouble(this.list.get(i));
		}

	}
	public void emptyList() {
		this.list.clear();
	}
	public static DoubleListWritable read(DataInput in) throws IOException {
		DoubleListWritable  w = new DoubleListWritable ();
		w.readFields(in);
		return w;
	}
	
	public void add(Double d) {
		this.list.add(d);
	}

	public List<Double> getList() {
		return this.list;
	}
	public void setList(List<Double> list) {
		this.list = list;
	}
}
