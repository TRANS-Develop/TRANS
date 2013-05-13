package TRANS.MR.Average.Mapper;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


import TRANS.Array.DataChunk;
import TRANS.MR.Average.StrideAverageResult;
import TRANS.MR.Median.StripeMedianResult;

public class StrideAverageMapper extends Mapper<IntWritable, StrideAverageResult, IntWritable, StrideAverageResult> {
	  public static enum InvalidCell { INVALID_CELL_COUNT } ;

	  Vector<Integer> fullIds = new Vector<Integer>();
	  DataChunk chunk = null;
	  private Counter c = null;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		c = (Counter)context.getCounter("TRASN_READ","MAP_READ");
		super.setup(context);
	}
	/**
	   * Reduces values for a given key
	   * @param key the Key for the given value being passed in
	   * @param value an Array to process that corresponds to the given key 
	   * @param context the Context object for the currently executing job
	   */
	  public void map(IntWritable key, StrideAverageResult value, Context context)
	                  throws IOException, InterruptedException {
		  	  c.increment(value.getSize());
		  	  System.out.println(value);
			  context.write(key, value);
	   }
}
