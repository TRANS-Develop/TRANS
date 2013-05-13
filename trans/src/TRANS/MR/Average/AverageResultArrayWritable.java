package TRANS.MR.Average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import TRANS.MR.Median.StripeMedianResult;

public class AverageResultArrayWritable implements Writable {

	public StrideAverageResult[] getResult() {
		return result;
	}
	public void setResult(StrideAverageResult[] result) {
		this.result = result;
	}
	private StrideAverageResult [] result = null;
	public AverageResultArrayWritable(){}
	public AverageResultArrayWritable(StrideAverageResult []r){
		this.result = r;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		int len = 0;
		if(result == null)
		{
			WritableUtils.writeVInt(out, 0);
			return;
		}
		len =result.length;
		WritableUtils.writeVInt(out,len);
		for(int i =0; i < len; i++)
		{
			result[i].write(out);
		}
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int len = WritableUtils.readVInt(in);
		if(len == 0)
		{
			return;
		}
		this.result = new StrideAverageResult[len];
		for(int i = 0; i <len; i++)
		{
			result[i] = new StrideAverageResult();
			result[i].readFields(in);
		}
	}

}
