package bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class RelationRecordReader extends RecordReader<Text, TupleWritable> {

    private FileSplit split;
    private LineRecordReader reader;
    private Text key;
    private TupleWritable value;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,
            InterruptedException {
        this.split = (FileSplit) inputSplit;
        this.reader = new LineRecordReader();
        this.reader.initialize(inputSplit, taskAttemptContext);
        this.value = new TupleWritable();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        String input = "";
        //Skip command lines
        do {
            boolean hasNext = this.reader.nextKeyValue();
            if (hasNext) {
                input = this.reader.getCurrentValue().toString();
            } else {
                return false;
            }
        } while (input.startsWith("#"));

        this.key = new Text(this.split.getPath().toString());
        ArrayList<String> list = new ArrayList<String>();
        StringTokenizer iter = new StringTokenizer(input, "\t");
        while (iter.hasMoreTokens()) {
            list.add(iter.nextToken());
        }
        this.value = new TupleWritable(list);
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public TupleWritable getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this.reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }
}