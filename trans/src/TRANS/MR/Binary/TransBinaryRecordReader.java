package TRANS.MR.Binary;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TransBinaryRecordReader extends RecordReader<Object,TransBinaryMapInputValue>{
	TransBinaryInputSplit split = null;
	String confDir = null;
	
	boolean readed = false;
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.split = (TransBinaryInputSplit)split;
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(readed){
			return false;
		}else{
			readed = true;
			return true;
		}
	}

	@Override
	public Object getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new IntWritable(1);
	}

	@Override
	public TransBinaryMapInputValue getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new TransBinaryMapInputValue(this.split);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
