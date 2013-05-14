package TRANS.Data.Reader;

import java.io.IOException;

public interface ByteReader {

	public  Object[] readData();
	public void readFromin() throws IOException;
	public void readFromin(int len) throws IOException;
}
