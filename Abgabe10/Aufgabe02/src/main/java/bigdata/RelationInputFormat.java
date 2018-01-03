package bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class RelationInputFormat extends FileInputFormat<Text, TupleWritable> {

    @Override
    public RecordReader<Text, TupleWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext
            taskAttemptContext) throws IOException, InterruptedException {
        RelationRecordReader reader = new RelationRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }
}