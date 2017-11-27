package bigdata;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RelationInputFormat extends FileInputFormat{
	
	public RecordReader createRecordReader(InputSplit split,  TaskAttemptContext context) throws IOException, InterruptedException {
		return new RelationRecordReader();
	}

}
