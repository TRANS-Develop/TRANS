package TRANS;

public class ArrayCreateStatus {
	private long lastUpdateStatus = -1;
	private long totalSize = 0;
	private long createdSize = 0;
	private double  creatingSpeed = 0; //MB/s
	
	public double getCreatingSpeed() {
		return creatingSpeed;
	}
	public void setCreatingSpeed(double creatingSpeed) {
		this.creatingSpeed = creatingSpeed;
	}
	public ArrayCreateStatus()
	{}
	public long getLastUpdateStatus() {
		return lastUpdateStatus;
	}
	public void setLastUpdateStatus(long lastUpdateStatus) {
		this.lastUpdateStatus = lastUpdateStatus;
	}
	public long getTotalSize() {
		return totalSize;
	}
	public void setTotalSize(long totalSize) {
		this.totalSize = totalSize;
	}
	public long getCreatedSize() {
		return createdSize;
	}
	public void setCreatedSize(long createdSize) {
		this.createdSize = createdSize;
	}
	public boolean isFull()
	{
		if(this.createdSize == this.totalSize)
		{
			return true;
		}
		return false;
	}
	
	
}
