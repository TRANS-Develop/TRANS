package TRANS.MR.Binary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import TRANS.Protocol.OptimusCatalogProtocol;

public class TransBinaryMapInputValue implements Writable {
	public TransBinaryInputSplit getSplit() {
		return split;
	}
	public void setSplit(TransBinaryInputSplit split) {
		this.split = split;
	}

	TransBinaryInputSplit split = null;
	public TransBinaryMapInputValue(){}
	public TransBinaryMapInputValue(TransBinaryInputSplit split)
	{
		this.split = split;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		split.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		split = new TransBinaryInputSplit();
		split.readFields(in);
	}

}
