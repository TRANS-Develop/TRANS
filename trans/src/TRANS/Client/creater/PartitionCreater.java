package TRANS.Client.creater;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import TRANS.Data.Writer.Interface.ByteWriter;
import TRANS.Exceptions.WrongArgumentException;

public abstract class PartitionCreater {
	protected DataOutputStream cout = null;
	protected DataInputStream cin = null; 
	
	abstract public ByteWriter getWriter()throws WrongArgumentException;
	abstract public boolean close();
}
