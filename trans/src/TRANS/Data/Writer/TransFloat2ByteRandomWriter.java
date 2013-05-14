package TRANS.Data.Writer;

import java.io.IOException;
import java.io.RandomAccessFile;

import TRANS.Array.Partition;
import TRANS.Data.Writer.Interface.ByteWriter;

public class TransFloat2ByteRandomWriter implements ByteWriter {
	static int eleSize = 4;
	public int size;
	byte[] data;
	int cur;
	private Partition p = null;
	RandomAccessFile rout = null;

	public TransFloat2ByteRandomWriter(int size, RandomAccessFile out,Partition p) {
		this.size = size;
		data = new byte[this.size * eleSize];
		this.rout = out;
		this.p = p;
	}


	public void write(Object f) throws IOException {
		if (cur >= size * eleSize) {
			this.rout.write(data);
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
			this.rout.write(data, 0, this.cur);
			this.data = new byte[fs.length * eleSize];
		}
		int i;
		for (int j = 0; j < fs.length; j++) {

			//long l = Double.doubleToLongBits(fs[j]);
			int l = Float.floatToIntBits((Float)fs[j]);
			for (i = 0; i < eleSize; i++) {
				data[cur++] = new Long(l).byteValue();// new
														// Integer(l).byteValue();
				l = l >> 8;
			}
		}

	}

	public void close() throws IOException {

		this.rout.write(data, 0, this.cur);
		//this.rout.close();
		p.close();
		//this.rout = null;

	}
}
