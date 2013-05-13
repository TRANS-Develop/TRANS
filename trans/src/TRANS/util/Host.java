package TRANS.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.RPC;

import TRANS.OptimusInstanceID;
import TRANS.Protocol.OptimusCalculatorProtocol;
import TRANS.Protocol.OptimusDataProtocol;

public class Host implements Writable,Comparable<Host> {

	OptimusConfiguration conf = null;
	
	private int dataPort = 0;
	private String host = null;
	private OptimusInstanceID instanceId = null;
	private boolean isDead = false;
	private long liveTime = 0;
	private int rplicatePort = 0;
	
	Socket socket = null;
	public Host() {}

	public Host( String host, int replicatePort, int dataPort, OptimusInstanceID instanceId) {
		this.host = host;
		this.rplicatePort = replicatePort;
		this.dataPort = dataPort;
		this.instanceId = instanceId;
		this.liveTime = System.currentTimeMillis();
	}

	public void closeReplicate() throws IOException {
		socket.close();
	}

	public int compareTo(Host ob) {

		if (this.host.equals(ob.host) && this.rplicatePort == ob.rplicatePort)
		{
			System.out.println(host+"Equals "+ob);
			return 0;
		}
		System.out.println(this+"Not Equals "+ob);
		System.out.println(this.host+":"+ob.host+"  "+this.host.equals(ob.host));
		System.out.println(this.rplicatePort+":"+ob.rplicatePort+"  "+(this.rplicatePort == ob.rplicatePort));
		
		return -1;
	}

	public void ConnectReplicate() throws UnknownHostException, IOException {
		socket = new Socket(this.host, this.rplicatePort);
		socket.setSendBufferSize(OptimusDefault.SOCKET_BUFFER_SIZE);
		
	}
	public boolean equals(Host host)
	{
		return (this.host.equals(host.host)&&this.rplicatePort== host.rplicatePort);
	}
	@Override
	public boolean equals(Object obj) {
		System.out.println("Calling Equals of host");
		
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Host other = (Host) obj;
		if (dataPort != other.dataPort)
			return false;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (instanceId == null) {
			if (other.instanceId != null)
				return false;
		} else if (!instanceId.equals(other.instanceId))
			return false;
		if (rplicatePort != other.rplicatePort)
			return false;
		return true;
	}

	public OptimusCalculatorProtocol getCalculateProtocol() throws IOException
	{
		//TODO
		
		return (OptimusCalculatorProtocol) RPC.waitForProxy(OptimusCalculatorProtocol.class,
				OptimusCalculatorProtocol.versionID,
				new InetSocketAddress(this.host,this.dataPort), new Configuration());

	}

	public OptimusDataProtocol getDataProtocol() throws IOException
	{
	
		return (OptimusDataProtocol) RPC.waitForProxy(OptimusDataProtocol.class,
				OptimusDataProtocol.versionID,
				new InetSocketAddress(this.host,this.dataPort), new Configuration());

	}

	public String getHost() {
		return host;
	}
	
	public OptimusInstanceID getInstanceId() {
		return instanceId;
	}
	public long getLiveTime() {
		return liveTime;
	}


	public int getPort() {
		return rplicatePort;
	}
	public DataInputStream getReplicateReply() throws IOException {
		return new DataInputStream(socket.getInputStream());
	}
	public DataOutputStream  getReplicateWriter() throws IOException {
		return new DataOutputStream(socket.getOutputStream());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + dataPort;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result
				+ ((instanceId == null) ? 0 : instanceId.hashCode());
		result = prime * result + rplicatePort;
		return result;
	}

	public boolean isDead() {
		return isDead;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
	

		this.instanceId =new OptimusInstanceID();
		this.instanceId.readFields(arg0);
		
		rplicatePort = WritableUtils.readVInt(arg0);
		host = WritableUtils.readString(arg0);
		this.dataPort = WritableUtils.readVInt(arg0);
		
		this.liveTime = WritableUtils.readVLong(arg0);
		this.isDead = WritableUtils.readVInt(arg0) == 1 ? true : false; 
		
		
	}

	public void setDead(boolean isDead) {
		this.isDead = isDead;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setInstanceId(OptimusInstanceID instanceId) {
		this.instanceId = instanceId;
	}

	public void setLiveTime(long liveTime) {
		this.liveTime = liveTime;
	}

	public void setPort(int port) {
		this.rplicatePort = port;
	}

	@Override
	public String toString() {
		return "Host [host=" + host + ", RplicatePort=" + rplicatePort
				+ ", instanceId=" + instanceId.toString() + "]";
	}

	public void update()
	{
		this.isDead = false;
		this.liveTime = System.currentTimeMillis();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
	

		this.instanceId.write(arg0);
		WritableUtils.writeVInt(arg0, rplicatePort);
		
		WritableUtils.writeString(arg0, host);
		WritableUtils.writeVInt(arg0,this.dataPort);
		this.liveTime = System.currentTimeMillis(); // update the live time
		WritableUtils.writeVLong(arg0, this.liveTime);
		WritableUtils.writeVInt(arg0, this.isDead ? 1 : 0);
	}

}
