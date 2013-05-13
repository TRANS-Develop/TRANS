package TRANS.MR.Average.combiner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import TRANS.MR.Average.StrideAverageResult;

public class StrideAverageCombiner extends Reducer<IntWritable, StrideAverageResult,IntWritable, StrideAverageResult> {
	
	public void reduce(IntWritable key, Iterable<StrideAverageResult> values,
			Context context) throws InterruptedException, IOException {
		Iterator<StrideAverageResult> it = values.iterator();
		StrideAverageResult result = it.next();
		if(result.isFull())
		{
			context.write(key, result);
		}else{
			while(it.hasNext())
			{
				result.addResult(it.next());
			}
			context.write(key, result);
		}
	}
}
