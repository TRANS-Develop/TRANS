package TRANS.Data.Writer;

import java.io.DataOutput;
import java.io.IOException;

import TRANS.Data.Writer.Interface.ByteWriter;

public class TransFloat2ByteStreamWriter implements ByteWriter {
	static int eleSize = 4;
	public int size;
	byte[] data;
	int cur;
	DataOutput dataout = null;

	public TransFloat2ByteStreamWriter(int size, DataOutput out) {
		this.size = size;
		data = new byte[this.size * eleSize];
		this.dataout = out;
	}
	
	public void write(Object f) throws IOException {
		if (cur >= size *eleSize) {
			this.dataout.write(data);
			this.cur = 0;
		}
		int l = Float.floatToIntBits((Float)f);
		for (int i = 0; i < eleSize; i++) {
			data[cur++] = new Integer(l).byteValue();// new Integer(l).byteValue();
			l = l >> 8;
		}
	}

	public void write(Object[] fs) throws IOException {
		if (this.size < fs.length) {
			this.close();
			this.data = new byte[fs.length * eleSize];
		}
		int i;
		for (int j = 0; j < fs.length; j++) {

			//long l = Double.doubleToLongBits(fs[j]);
			int l = Float.floatToIntBits((Float)fs[j]);
			for (i = 0; i < eleSize; i++) {
				data[cur++] = new Long(l).byteValue();// new Integer(l).byteValue();
				l = l >> 8;
			}
		}

	}
	public void close() throws IOException {

		this.dataout.write(data, 0, this.cur);

	}
}
