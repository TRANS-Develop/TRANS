package TRANS.MR.Average.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import TRANS.Data.OptimusData;
import TRANS.MR.io.AverageResult;

public class AveragePushDownMapper extends Mapper<Object, AverageResult, LongWritable, AverageResult> {
		 
		 private Counter c = null;
		  @Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			  c = (Counter)context.getCounter("TRANS_READ", "MAP_READ");
			// TODO Auto-generated method stub
			super.setup(context);
		}
		/**
		   * Reduces values for a given key
		   * @param key the Key for the given value being passed in
		   * @param value an Array to process that corresponds to the given key 
		   * @param context the Context object for the currently executing job
		   */
		  public void map(Object key, AverageResult value, Context context)
		                  throws IOException, InterruptedException {
		     c.increment(value.getSize());
		    
		     context.write(new LongWritable(1), value);
		   }
		     
}
