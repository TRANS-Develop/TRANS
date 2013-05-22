package TRANS.MR.Average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import TRANS.Data.TransDataType;
import TRANS.MR.io.AverageResult;

public class StrideAverageResult extends AverageResult {
	@Override
	public void write(DataOutput out) throws IOException {
		
		// TODO Auto-generated method stub
		WritableUtils.writeVInt(out, id);
		WritableUtils.writeVInt(out, this.volume);
		super.write(out);
	}
	
	@Override
	public long getSize() {
		// TODO Auto-generated method stub
		return super.getSize() + 4;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.id = WritableUtils.readVInt(in);
		this.volume = WritableUtils.readVInt(in);
		super.readFields(in);
	}
	private int id = 0;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	private int volume = 1;
	private boolean full = false;
	private int []start = null;
	private int []stride = null;
	public int getVolume() {
		return volume;
	}
	public void setVolume(int volume) {
		this.volume = volume;
	}
	public boolean isFull() {
		return (this.volume == super.get_valuesCombinedCount());
	}
	public void setFull(boolean full) {
		this.full = full;
	}
	public int[] getStart() {
		return start;
	}
	public void setStart(int[] start) {
		this.start = start;
	}
	public int[] getStride() {
		return stride;
	}
	public void setStride(int[] stride) {
		this.stride = stride;
	}
	
	public boolean isOverlaped( int []start, int []off)
	{
		boolean isIntersect = true;
		for (int i = 0; i < stride.length; i++) {
			if ((this.start[i] >= start[i] + off[i] || this.start[i]
					+ this.stride[i] <= start[i])) {
				isIntersect = false;
			}
		}
		return isIntersect;
	}
	public StrideAverageResult(){}
	public StrideAverageResult(Class<?>type,int id,int []start, int []stride) throws IOException
	{
		this.id = id;
		for(int i=0; i < stride.length; i++)
		{
			volume *= stride[i];
		}
		this.start = start;
		this.stride = stride;
		super.type = new TransDataType(type);
	}
	
	
}
