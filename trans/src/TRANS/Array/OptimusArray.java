package TRANS.Array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import TRANS.Data.TransDataType;


public class OptimusArray implements Writable{
	public TransDataType getType() {
		return type;
	}

	public void setType(TransDataType type) {
		this.type = type;
	}

	public enum ARRAY_STATUS{
		UNDEFINED,
		CREATING,
		FINISHED,
		DELETED,
		FAILED
	}
	@Override
	public String toString() {
		return "OptimusArray [zid=" + zid + ", id=" + id + ", name=" + name
				+ ", devalue=" + devalue + ", arrayState=" + arrayState
				+ ", overlap=" + overlap + "]";
	}

	public OptimusShape getOverlap() {
		return overlap;
	}
	
	private TransDataType type = new TransDataType();
	private ZoneID zid = null;
	private ArrayID id = null;
	private String name = null;
	private float devalue = 0;
	private ARRAY_STATUS arrayState = ARRAY_STATUS.UNDEFINED;
	private OptimusShape overlap = new OptimusShape();
	

	public OptimusArray()	{}
	
	public OptimusArray(ZoneID zid,ArrayID id,String name,float devalue,TransDataType type)
	{
		this.id = id;
		this.zid = zid;
		this.name = name;
		this.type = type;
		this.devalue = devalue;
		this.arrayState = ARRAY_STATUS.UNDEFINED;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		
		this.type.readFields(arg0);
		this.id = new ArrayID();
		id.readFields(arg0);
		this.zid = new ZoneID();
		this.zid.readFields(arg0);
		int status = arg0.readInt();
		this.arrayState = ARRAY_STATUS.UNDEFINED;
		for(ARRAY_STATUS stat :ARRAY_STATUS.values())
		{
			if(stat.ordinal() == status)
			{
				this.arrayState = stat;
				break;
			}
		}
		
		this.devalue = arg0.readFloat();
		this.name = WritableUtils.readString(arg0);
		this.overlap.readFields(arg0);
		
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		this.type.write(arg0);
		this.id.write(arg0);
		this.zid.write(arg0);
		arg0.writeInt(this.arrayState.ordinal());
		//arg0.writeBoolean(this.arrayState);
		arg0.writeFloat(this.devalue);
		WritableUtils.writeString(arg0, this.name);
		this.overlap.write(arg0);
	}
	
	public ArrayID getId() {
		return id;
	}



	public void setId(ArrayID id) {
		this.id = id;
	}

	public ARRAY_STATUS isDeleted() {
		return arrayState;
	}

	public void setDeleted(ARRAY_STATUS status) {
		this.arrayState = status;
	}

	public ZoneID getZid() {
		return zid;
	}



	public void setZid(ZoneID zid) {
		this.zid = zid;
	}



	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setOverlap(OptimusShape overlap) {
		this.overlap = overlap;
	}

	
}
