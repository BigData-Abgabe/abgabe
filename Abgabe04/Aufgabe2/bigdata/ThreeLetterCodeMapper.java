package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class ThreeLetterCodeMapper extends Mapper<Object, Text, Text, Text> {
	private Text prot = new Text();
	private Text aa = new Text();
	
	private Configuration conf;
	private BufferedReader fis;
	Map<String, String> letterCode = new HashMap<String, String>();
	
	static enum CountersEnum{ AA, Proteins }
	
	
	@Override
	public void setup(Context context) 
			throws IOException, InterruptedException {
		conf= context.getConfiguration();
		
	URI[] codeURIs= Job.getInstance(conf).getCacheFiles();
	for(URI codeURI: codeURIs) {
		Path patternsPath= new Path(codeURI.getPath());
		String patternsFileName= patternsPath.getName().toString();
		parseAcidFile(patternsFileName);
		}
	}
	
	//parses Aminosäure File
	private void parseAcidFile(String fileName) {
		try{
			fis= new BufferedReader(new FileReader(fileName));
			String line= null;
			String[] map;
			while((line= fis.readLine()) != null) {
				map= line.split("/t");
				letterCode.put(map[0],map[1]);
			}
		} catch (IOException e) {
			System.err.println("Caughtexceptionwhileparsingthecachedfile'"+ StringUtils.stringifyException(e));
		}
	}
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		   
		String protein = value.toString();;
		String threeCode= "";
		 
		//Counts proteins
		Counter proteins= context.getCounter(CountersEnum.class.getName(),CountersEnum.AA.toString());
		proteins.increment(1);
		
		for (int i = 0; i < protein.length();i++){
			
			//counts aminoacids
			Counter amino = context.getCounter(CountersEnum.class.getName(),CountersEnum.AA.toString());
			amino.increment(1);
			
			// converts one letter code in three letter code if pattern is given
			String tmp = letterCode.get( protein.charAt(i) );
			if ( tmp != null ){
				threeCode += tmp;  
			}
			else
				System.out.println( protein.charAt(i)+" is not inplemented in Aminosäuredatei" );
		}
		
		aa.set(key.toString());
		prot.set(threeCode);

		context.write(aa, prot);
		
	}
}