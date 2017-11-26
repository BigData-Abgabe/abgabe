package bigdata;

import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


import java.io.IOException;


public class RelationRecordReader extends RecordReader<Text,TupleWritable> {
    private Text path;
    private LineRecordReader lineRecordReader = new LineRecordReader();
    private TupleWritable value;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) inputSplit;
       /* Configuration job = taskAttemptContext.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        start = fileSplit.getStart();
        end = fileSplit.getLength() + start;*/
       lineRecordReader.initialize(inputSplit,taskAttemptContext);
       this.path = new Text(fileSplit.getPath().toString());

      /*  final Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(file);
        value.readFields(fileIn);
*/

    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        return lineRecordReader.nextKeyValue();
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text(path);
    }

    public TupleWritable getCurrentValue() throws IOException, InterruptedException {
        Text text = lineRecordReader.getCurrentValue();
        if(text.toString().startsWith("#")){
            lineRecordReader.nextKeyValue();
            return getCurrentValue();
        }
        value = new TupleWritable(text);
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
    return lineRecordReader.getProgress();
    }

    public void close() throws IOException {
        lineRecordReader.close();
    }
}
