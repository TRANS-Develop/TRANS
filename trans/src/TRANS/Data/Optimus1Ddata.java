package TRANS.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Optimus1Ddata implements Writable{

	public Object[] getData() {
		return data;
	}
	public void setData(Object[] data) {
		this.data = data;
	}
	Object [] data = null;
	public Optimus1Ddata(){}
	public Optimus1Ddata(Object []data)
	{
		this.data = data;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		if(data == null)
		{
			out.writeInt(0);
			return;
		}
		out.writeInt(data.length);
		for(int i = 0 ; i < data.length; i++)
		{
			//out.writeFloat(data[i]);
			//out.writeDouble(data[i]);
		}
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int len =	in.readInt();
		if(len == 0)
		{
			this.data = null;
			return ;
		}
	//	this.data = new double[len];
		for(int i = 0; i < len; i++)
		{
			this.data[i]  = in.readDouble();
		}
	}
	
}
