package TRANS.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Writable;

import TRANS.Array.ArrayID;

public class TRANSWritableArray implements Writable{
	public Vector<ArrayID> getElements() {
		return elements;
	}
	Vector<ArrayID> elements = new Vector<ArrayID>();
	
	public void add(ArrayID t)
	{
		elements.add(t);
	}
	public void setElements(Vector<ArrayID> eles)
	{
		this.elements = eles;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(elements.size());
		for(ArrayID t: elements)
		{
			t.write(out);
		}
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int len = in.readInt();
		
		for(int i = 0; i < len; i++ )
		{
			
			ArrayID tmp = new ArrayID();
			tmp.readFields(in);
			this.elements.add(tmp);
		}
	}
	

}
