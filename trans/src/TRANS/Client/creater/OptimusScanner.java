package TRANS.Client.creater;

import java.io.IOException;
import java.util.Vector;

import TRANS.Array.DataChunk;

public interface OptimusScanner{
	public  Object[] readChunkData(DataChunk chunk, String name);
	public Class<?> getElementType(String name)throws IOException ;
}
