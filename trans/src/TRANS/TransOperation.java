package TRANS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Writable;

public class TransOperation {
	
	private TRANSOP op = null;
	private Vector<Writable> args = null; 
	public TransOperation(TRANSOP o,Vector<Writable> ops)
	{
		this.op = o;
		this.args = ops;
	}

	public void write(DataOutput out) throws IOException {
		
		out.writeInt(op.ordinal());
		for(Writable w:args)
		{
			w.write(out);
		}
	}
	
}
