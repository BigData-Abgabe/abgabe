package bigdata;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class DoubleListWritable implements Writable,Iterable<Double> {
          
       private List<Double> list = new ArrayList<Double>();

       @Override
       public void write(DataOutput out) throws IOException {
		  for (double d:list)       
			out.writeDouble(d);
       }
       
       @Override
       public void readFields(DataInput in) throws IOException {
         
    	 while(true) {
    		 try {
    			 list.add(in.readDouble());
    		 } catch (EOFException eofex) {
    			 break;
    		 }
    	 }
         
       }
       
       public static DoubleListWritable read(DataInput in) throws IOException {
         DoubleListWritable  w = new DoubleListWritable ();
         w.readFields(in);
         return w;
       }

 		public List<Double> getList() {
         return list;
         
       }
		
		public void add(double d) {
         list.add(d);
         
       }

		public void setList(double... doubles) {
		 for (double d: doubles)
         	list.add(d);
         
       }

		@Override
		public Iterator<Double> iterator() {
			return list.iterator();
		}

     }
