package TRANS.Data.Writer;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import TRANS.Data.Writer.Interface.ByteWriter;

public class OptimusDouble2ByteStreamWriter implements ByteWriter {

	public int size;
	byte[] data;
	int cur;
	DataOutput dataout = null;

	public OptimusDouble2ByteStreamWriter(int size, DataOutput out) {
		this.size = size;
		data = new byte[this.size * 8];
		this.dataout = out;
	}
	
	
	public void write(Object f) throws IOException {
		if (cur >= size * 8) {
			this.dataout.write(data);
			this.cur = 0;
		}
		long l = Double.doubleToLongBits((Double)f);

		for (int i = 0; i < 8; i++) {
			data[cur++] = new Long(l).byteValue();// new Integer(l).byteValue();
			l = l >> 8;
		}
	}

	public void write(Object[] fs) throws IOException {
		if (this.size < fs.length) {
			this.close();
			this.data = new byte[fs.length * 8];
		}
		int i;
		for (int j = 0; j < fs.length; j++) {

			long l = Double.doubleToLongBits((Double)fs[j]);
			for (i = 0; i < 8; i++) {
				data[cur++] = new Long(l).byteValue();// new
														// Integer(l).byteValue();
				l = l >> 8;
			}
		}

	}

	public void close() throws IOException {

		this.dataout.write(data, 0, this.cur);

	}

}
