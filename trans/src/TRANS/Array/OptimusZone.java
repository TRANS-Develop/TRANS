package TRANS.Array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


public class OptimusZone implements Writable {

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OptimusZone other = (OptimusZone) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}
	private String name = null;
	@Override
	public String toString() {
		return "OptimusZone [name=" + name + ", id=" + id + ", size=" + size
				+ ", pstep=" + pstep + ", strategy=" + strategy + "]";
	}
	private ZoneID id = null;
	private OptimusShape size = null; // 閿熸枻鎷烽敓鏂ゆ嫹鎷囬敓鍙拷姣忛敓鏂ゆ嫹zone閿熷彨纰夋嫹閿熸枻鎷烽敓浠嬮兘閿熸枻鎷蜂竴閿熸枻鎷锋媷閿熷彨锟�	private OptimusShape pstep = null;// partition閿熶茎杈炬嫹灏�
	private OptimusShapes strategy = null; // 閿熸枻鎷烽敓鏂ゆ嫹閿熸枻鎷烽敓锟�	
	private OptimusShape pstep = null;
	public int getReplicaNumber()
	{
		return this.strategy.getShapes().size() - 1;
	}
	public OptimusZone(){}
	public OptimusZone(String name,ZoneID id,OptimusShape size,OptimusShape step, OptimusShapes strategy)
	{
		this.name = name;
		this.id = id;
		this.size = size;
		this.strategy = strategy;
		this.pstep = step;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		
		WritableUtils.writeString(out, name);
		id.write(out);
		this.size.write(out);
		this.strategy.write(out);
		this.pstep.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.name = WritableUtils.readString(in);
		this.id = new ZoneID();
		this.id.readFields(in);
		this.size = new OptimusShape();
		this.size.readFields(in);
		this.strategy = new OptimusShapes();
		this.strategy.readFields(in);
		this.pstep = new OptimusShape();
		this.pstep.readFields(in);
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public ZoneID getId() {
		return id;
	}
	public void setId(ZoneID id) {
		this.id = id;
	}
	public OptimusShape getSize() {
		return size;
	}
	public void setSize(OptimusShape size) {
		this.size = size;
	}
	public OptimusShape getPstep() {
		return pstep;
	}
	public void setPstep(OptimusShape pstep) {
		this.pstep = pstep;
	}
	public OptimusShapes getStrategy() {
		return strategy;
	}
	public void setStrategy(OptimusShapes strategy) {
		this.strategy = strategy;
	}

}
