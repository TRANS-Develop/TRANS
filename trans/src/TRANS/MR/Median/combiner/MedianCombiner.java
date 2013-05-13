package TRANS.MR.Median.combiner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import TRANS.MR.Median.StripeMedianResult;


public class MedianCombiner extends Reducer<IntWritable, StripeMedianResult,IntWritable, StripeMedianResult> {
	private Counter c = null;
	private Counter cf = null;
	private Counter cnf = null;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		cf = (Counter)context.getCounter("INMEDIATE_DATA","COMBINER_OUPUT_FULL");
		cnf = (Counter)context.getCounter("INMEDIATE_DATA","COMBINER_OUPUT_NOT_FULL");
		super.setup(context);
	}

	public void reduce(IntWritable key, Iterable<StripeMedianResult> values,
			Context context) throws InterruptedException, IOException {
		Iterator<StripeMedianResult> it = values.iterator();
		StripeMedianResult result = it.next();
		if(result.isFull())
		{
			context.write(key, result);
			cf.increment(1);
		}else{
			while(it.hasNext())
			{
				result.add(it.next());
			}
			if(result.isFull())
			{
				cf.increment(1);
			}else{
				cnf.increment(1);
			}
			context.write(key, result);
		}
	}
}
