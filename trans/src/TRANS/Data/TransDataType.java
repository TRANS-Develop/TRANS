package TRANS.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TransDataType implements Writable{
	public enum TYPE{
		TRANS_DOUBLE,
		TRANS_FLOAT,
		NOT_DEFINED
	}
	private TYPE t = TYPE.NOT_DEFINED; 
	
	public TYPE getT() {
		return t;
	}

	public void setT(TYPE t) {
		this.t = t;
	}
	public TransDataType(){}
	public TransDataType(Class<?> type)throws IOException
	{
		if(type.equals(Double.class))
		{
			this.t = TYPE.TRANS_DOUBLE;
		}else if(type.equals(Float.class))
		{
			this.t = TYPE.TRANS_FLOAT;
		}else{
			System.out.print("Unknown Type"+type.toString());
			throw new IOException("Unknown Type"+type.toString());
		}
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		t = WritableUtils.readEnum(arg0,TYPE.class);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeEnum(arg0, t);
		
	}
	static public Class<?> getClass(TransDataType type)throws IOException
	{
		TYPE tmp = type.getT();
		if(tmp.equals(TYPE.TRANS_DOUBLE))
		{
			return Double.class;
		}else if(tmp.equals(TYPE.TRANS_FLOAT))
		{
			return Float.class;
		}else{
			System.out.println("Unsupported Type");
			throw new IOException("Unsupported Type");
		}
	}
	public int getElementSize()throws IOException
	{
		if(this.t.equals(TYPE.TRANS_DOUBLE))
		{
			return 8;
		}else if(t.equals(TYPE.TRANS_FLOAT))
		{
			return 4;
		}else{
			System.out.println("Unsupported Type");
			throw new IOException("Unsupported Type");
		}
	}
}
