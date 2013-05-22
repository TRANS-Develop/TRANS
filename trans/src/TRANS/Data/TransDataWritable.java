package TRANS.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public abstract class TransDataWritable implements Writable{
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(data);
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TransDataWritable other = (TransDataWritable) obj;
		if (!Arrays.equals(data, other.data))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}
	protected TransDataType type = new TransDataType();
	protected Object [] data = null;
	
	protected void writeDouble(DataOutput out) throws IOException 
	{
		//Double []ddata = (Double [])this.data;
		for(int i = 0 ; i < this.data.length; i++)
		{
			//out.writeFloat(this.data[i]);
			out.writeDouble((Double)data[i]);
		}
	}
	protected void readDouble(DataInput in)throws IOException
	{
		for(int i = 0 ; i < this.data.length; i++)
		{
			this.data[i] = in.readDouble();
		}
	}
	protected void writeFloat(DataOutput out) throws IOException 
	{
		//Float []ddata = (Float [])this.data;
		for(int i = 0 ; i < this.data.length; i++)
		{
			out.writeFloat((Float)data[i]);
		}
	}
	protected void readFloat(DataInput in)throws IOException
	{
		for(int i = 0 ; i < this.data.length; i++)
		{
			this.data[i] = in.readFloat();
		}
	}
	protected void writeInteger(DataOutput out) throws IOException 
	{
		//Integer []ddata = (Integer [])this.data;
		for(int i = 0 ; i < this.data.length; i++)
		{
			out.writeInt((Integer)data[i]);
		}
	}
	protected void readInteger(DataInput in)throws IOException
	{
		for(int i = 0 ; i < this.data.length; i++)
		{
			this.data[i] = in.readInt();
		}
	}
	
	public void write(DataOutput out) throws IOException {
		this.type.write(out);
		if(this.data == null)
		{
			out.writeInt(0);
			return;
		}
		out.writeInt(this.data.length);
		Class<?> dc = TransDataType.getClass(type);
		if(dc.equals(Double.class))
		{
			this.writeDouble(out);
		}else if(dc.equals(Float.class))
		{
			this.writeFloat(out);
		}else if(dc.equals(Integer.class))
		{
			this.writeInteger(out);
		}else{
			System.out.println("Unknown Data Type@TRANSDATA Write");
			throw new IOException("Unknown Data Type");
		}
	}
	public void readFields(DataInput in) throws IOException{
		this.type.readFields(in);
		int l = in.readInt();
		this.data = new Object[l];
		Class<?> dc = TransDataType.getClass(type);
		if(dc.equals(Double.class))
		{
			this.readDouble(in);
		}else if(dc.equals(Float.class))
		{
			this.readFloat(in);
		}else if(dc.equals(Integer.class))
		{
			this.readInteger(in);
		}else{
			throw new IOException("Unknown Data Type@transdata read");
		}
	}
}
