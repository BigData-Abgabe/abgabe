package bigdata;

import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RelationRecordReader extends RecordReader<Text,TupleWritable>{

	private LineRecordReader lineRR = new LineRecordReader();
	private TupleWritable tuple;
	private Text key;
	

	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException{
		
		lineRR.initialize(split, context);
		FileSplit fs = (FileSplit) split;
		key = new Text(fs.getPath().toString());
	}
	
	@Override
	public boolean nextKeyValue() throws IOException{
		
		return lineRR.nextKeyValue(); 
	}
	
	@Override
	public TupleWritable getCurrentValue() throws IOException{
		
		String line =lineRR.getCurrentValue().toString();
		
		if (line.startsWith("#")){
			nextKeyValue();
			return getCurrentValue();
		}
		
		tuple = new TupleWritable(line);
		return tuple;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		
		return key;
	}
	
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineRR.getProgress();
	}
	
	@Override
	public void close() throws IOException{
		lineRR.close();
	}






	
}
