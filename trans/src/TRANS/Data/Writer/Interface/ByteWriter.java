package TRANS.Data.Writer.Interface;

import java.io.IOException;

public interface ByteWriter {
	public void write(Object d)throws IOException;
	public void write(Object []d)throws IOException;
	public void close() throws IOException;
}
