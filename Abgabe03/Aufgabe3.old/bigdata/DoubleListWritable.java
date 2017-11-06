package bigdata;


java.util.ArrayList

public class DoubleListWritable implements Writable {
          
       private List<Double> list = new ArrayList<Double>;

       
       public void write(DataOutput out) throws IOException {
		  for (double d:list)       
			out.writeInt(d);
         
       }
       
       public void readFields(DataInput in) throws IOException {
         
		 String tmp = in.readLine();
		 String[] s = tmp.split('/t')
		 for (i = 1; s.length(); i++)
			list.add(Double.parseDouble(s[i]))
         
       }
       
       public static DoubleListWritable read(DataInput in) throws IOException {
         DoubleListWritable  w = new DoubleListWritable ();
         w.readFields(in);
         return w;
       }

 		public void getList() {
         return list;
         
       }
		
		public void add(double d) {
         list.add(d);
         
       }

		public void setList(double... doubles) {
		 for (double d: doubles
         	list.add(d);
         
       }

     }
