package TRANS.Array;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Set;
import java.util.TreeSet;

public class DataChunk implements Comparable<DataChunk> {

	int chunkNum;

	private int[] chunkNumMove = null;

	private int[] chunkStep;

	int inChunk; // ��chunk�ڲ���offset

	private int size;
	private int[] start = null;
	private int[] vsize = null;
	private int[] chunkSize = null;
	private int[] overlap = null;
	public int[] getOverlap() {
		return overlap;
	}
	public void setOverlap(int[] overlap) {
		this.overlap = overlap;
	}

	private int offset = -1;
	public DataChunk(DataChunk chunk)
	{
		this(chunk.getVsize(),chunk.getChunkStep());
		int [] tstart = chunk.getStart();
		for(int i = 0 ; i < start.length ; i++)
		{
			start[i] = tstart[i];
		}
		this.overlap = chunk.getOverlap();
		this.chunkNum = chunk.getChunkNum();
	}
	public DataChunk(int[] vsize, int[] chunkStep) {
		this.vsize = vsize; // varible size
		this.chunkStep = chunkStep; //
		this.start = new int[this.vsize.length];
		this.size = 1;
		for (int i = 0; i < this.vsize.length; i++) {
			this.size *= this.chunkStep[i];
		}
	}


	@Override
	public String toString() {
		return "DataChunk [chunkNum=" + chunkNum + ", chunkNumMove="
				+ Arrays.toString(chunkNumMove) + ", chunkStep="
				+ Arrays.toString(chunkStep) + ", inChunk=" + inChunk
				+ ", size=" + size + ", start=" + Arrays.toString(start)
				+ ", vsize=" + Arrays.toString(vsize) + ", chunkSize="
				+ Arrays.toString(chunkSize) + ", overlap="
				+ Arrays.toString(overlap) + ", offset=" + offset + "]";
	}
	@Override
	public int compareTo(DataChunk chunk) {
		DataChunk arg0 = (DataChunk)chunk;
		for (int i = arg0.getStart().length - 1; i >= 0; --i) {
			if (this.getStart()[i] == arg0.getStart()[i])
				continue;
			return this.getStart()[i] < arg0.getStart()[i] ? -1 : 1;
		}
		return 0;
		
	}

	/*
	 * �������chunk �Ƿ����
	 */
	public boolean equals(DataChunk chunk) {
		for (int i = 0; i < this.chunkStep.length; i++) {

			if (chunk.getStart()[i] != this.start[i]
					|| chunk.getChunkStep()[i] != this.chunkStep[i])
				return false;
		}
		System.out.println("Equal");
		return true;

	}

	public boolean equals(Object obj) {
		
		if (obj.getClass().equals(this.getClass())) {
			return this.equals((DataChunk) obj);
		} else {
			return super.equals(obj);
		}
	}

	/*
	 * ��ȡ��һ����Χ��ص�����chunk
	 */
	public Set<DataChunk> getAdjacentChunks(int[] start, int[] off) 
	{
		long startPos = 0, rsize = 1,l = 1;
		for (int i = start.length -1 ; i >= 0; i--)
		{
			startPos += l*start[i] ;
			l*= this.vsize[i];
			rsize *= off[i];
		}
		if (rsize == 0) { 
			return null;
		}
		
		Set<DataChunk> chunks = new TreeSet<DataChunk>();
		Set<DataChunk> pass = new TreeSet<DataChunk>();
		Deque<DataChunk> cque = new ArrayDeque<DataChunk>();
		
		DataChunk chunk = new DataChunk(this.vsize, this.chunkStep);
		chunk.getChunkByOff(startPos);

		chunks.add(chunk);
		cque.add(chunk);
		pass.add(chunk);
		
		DataChunk tmp = null;
		while (!cque.isEmpty()) {
			chunk = cque.getFirst();
			cque.removeFirst();
			for (int i = 0; i < chunk.getStart().length; i++) {
				tmp = chunk.moveUp(i);
				if (tmp != null && !pass.contains(tmp))
				{
					if(tmp.isOverlaped(start, off)) {
						cque.addLast(tmp);
						chunks.add(tmp);
					}
					pass.add(tmp);
				}
			
				tmp = chunk.moveDown(i);
				if (tmp != null && !pass.contains(tmp))
				{
					if(tmp.isOverlaped(start, off)) {
						cque.addLast(tmp);
						chunks.add(tmp);
					}
					pass.add(tmp);
				}
			}
			
		}
		return chunks;
	}

	/*
	 * ���һ������partition�е�λ���ж��Ƿ�������chunk
	 */
	public void getChunkByOff(long off) {
		//TODO
		int[] rpos = new int[vsize.length];
		long toff = off;
		for (int i = vsize.length -1  ; i >=0 ; i--) {
			rpos[i] =(int)(toff % vsize[i]);
			toff /= vsize[i];
		}
		int cnum = 0;
		int ichunk = 0;
		for (int i = 0 ; i < vsize.length ; i++) {
			cnum = cnum
					*(( vsize[i]
					/ (this.chunkStep[i] )+ (vsize[i] % this.chunkStep[i] == 0 ? 0
							: 1))) + rpos[i] / this.chunkStep[i];
			
			this.start[i] = rpos[i] - rpos[i] % this.chunkStep[i];
		}
		int []csize = this.getChunkSize();
		for(int i = 0; i < csize.length; i++)
		{
			ichunk = rpos[i] % csize[i] + ichunk * csize[i];
		}
		this.chunkNum = cnum;
		this.inChunk = ichunk;

	}

	public int getChunkNum() {
		return chunkNum;
	}

	/*
	 * �ڵ�i���������ƶ�һ��chunknum�ĸĶ�
	 */
	public int getChunkNumMove(int i) {
		if (this.chunkNumMove == null) {
			this.initchunkMove();
		}
		return this.chunkNumMove[i];
	}

	public int[] getChunkStep() {
		return this.chunkStep;
	}
	public int[] getChunkSize()
	{
		
		int [] csize = new int[this.vsize.length];
		if( this.overlap == null)
		{
			for(int i = 0 ; i < vsize.length; i++)
			{
				csize[i] = (vsize[i] - start[i]) > this.chunkStep[i] ? this.chunkStep[i]:(vsize[i] - start[i]);
			}
		}else{
			int s,o;
			for(int i = 0 ; i < vsize.length; i++)
			{
				s = start[i] - overlap[i] > 0 ? start[i] - overlap[i]:0;
				o = chunkStep[i] + 2*overlap[i];
				csize[i] = (vsize[i] - s) > o ? o: vsize[i] - s;
			}
		}
		return csize;
	}

	public int getInChunk() {
		return inChunk;
	}
	public int getOffset() {
		return this.getChunkOffset() + this.inChunk;
	}

	public int getSize() {
		int size = 1;
		if(overlap == null){
		for(int i = 0 ; i < vsize.length; i++)
		{
			size *= (vsize[i] - start[i]) > this.chunkStep[i] ? this.chunkStep[i]:(vsize[i] - start[i]);
		}}else{
			int s,o;
			for(int i = 0 ; i < vsize.length; i++)
			{
				s = start[i] - overlap[i] > 0 ? start[i] - overlap[i]:0;
				o = chunkStep[i] + 2*overlap[i];
				size *= (vsize[i] - s) > o ? o: vsize[i] - s;
			}
		}
		return size;
	}

	public int[] getStart() {
		return start;
	}

	/*
	 * chunk �еĵ�һ������partition�е�λ��
	 */
	//public int getChunkPos() {
	//	return this.getSize()*this.chunkNum;
	//}

	public int[] getVsize() {
		return vsize;
	}

	/*
	 * �Ƿ�����һ��chunk
	 */
	public boolean hasNext() {
		int tmp = vsize.length - 1;
		while (tmp >=0
				&& this.start[tmp] + this.chunkStep[tmp] >= this.vsize[tmp]) {
			tmp--;
		}
		if (tmp < 0) {
			return false;
		}
		return true;

	}

	private void initchunkMove() {
		chunkNumMove = new int [this.vsize.length];
		chunkNumMove[vsize.length  - 1] = 1;
		;
		for (int i = vsize.length  - 2; i >= 0; i--) {
			this.chunkNumMove[i] = this.chunkNumMove[i + 1]
					* (this.vsize[i + 1] / this.chunkStep[i + 1]
							+ (this.vsize[i + 1] % this.chunkStep[i + 1] == 0 ? 0
							: 1));
		}
		return;
	}
	public int getTotalChunk()
	{
		int m = this.getChunkNumMove(0);
		return m* (this.vsize[0] / this.chunkStep[0]
						+ (this.vsize[0] % this.chunkStep[0] == 0 ? 0
						: 1));
	}
	/*
	 * �Ƿ���һ���������ཻ
	 */
	private boolean isOverlaped(int[] start, int[] off) {
		int len = this.start.length;
		
		boolean isIntersect = true;
		for (int i = 0; i < len; i++) {
			if ((this.start[i] >= start[i] + off[i] || this.start[i]
					+ this.chunkStep[i] <= start[i])) {
				isIntersect = false;
			}
		}
		return isIntersect;
	}

	/*
	 * ����chunk�Ƿ��ཻ
	 */
	/*public boolean isOverlaped(DataChunk chunk) {
		int len = this.start.length;
		for (int i = 0; i < len; i++) {
			if (!(this.start[i] >= (chunk.getStart()[i] + chunk.getChunkStep()[i]) || this.start[i]
					+ this.chunkStep[i] < chunk.getStart()[i])) {
				return true;
			}
		}
		return false;
	}
*/
	public DataChunk moveDown(int i) {
		if (this.start[i] - this.chunkStep[i] < 0)
			return null;
		DataChunk chunk = new DataChunk(this.vsize, this.chunkStep);
		for (int j = 0; j < this.vsize.length; j++) {
			chunk.setChunkStep(this.chunkStep[j], j);
			chunk.setStart(this.start[j], j);
		}
		chunk.setStart(this.start[i] - this.chunkStep[i], i);
		chunk.setChunkNum(this.chunkNum - this.getChunkNumMove(i));
		chunk.setOverlap(overlap);
		return chunk;
	}

	public DataChunk moveUp(int i) {
		if (this.start[i] + this.chunkStep[i] >= this.vsize[i])
			return null;
		DataChunk chunk = new DataChunk(this.vsize, this.chunkStep);
		for (int j = 0; j < this.vsize.length; j++) {
			chunk.setChunkStep(this.chunkStep[j], j);
			chunk.setStart(this.start[j], j);
		}
		chunk.setStart(this.start[i] + this.chunkStep[i], i);
		chunk.setChunkNum(this.chunkNum + this.getChunkNumMove(i));
		chunk.setOverlap(overlap);
		return chunk;
	}

	public boolean nextChunk() {
		if (!this.hasNext()) {
			return false;
		}
		int len = vsize.length -1;
		int tmp = len;
		while (tmp >= 0
				&& this.start[tmp] + this.chunkStep[tmp] >= this.vsize[tmp]) {
			tmp--;
		}
		this.start[tmp] += this.chunkStep[tmp];
		for (int i = len; i > tmp; i--) {
			this.start[i] = 0;
		}
		this.chunkNum++;
		return true;
	}

	public boolean nextPos() {
		if (this.inChunk < this.size - 1) {
			this.inChunk++;
			return true;
		} else {
			if (this.nextChunk()) {
				this.inChunk = 0;
				return true;
			}
			return false;
		}
	}

	/*
	 * ��chunk��һ��
	 */
	public void rewind() {
		
		this.inChunk = 0;
	}
	// ����һ��chunk
	public void reset()
	{
		this.inChunk = 0;
		this.chunkNum = 0;	
		for(int i = 0 ; i  < this.start.length; i++)
		{
			this.start[i] = 0;
		}
	}

	public void setChunkNum(int chunkNum) {
		this.chunkNum = chunkNum;
	}

	public void setChunkStep(int step, int i) {
		this.chunkStep[i] = step;
	}

	public void setChunkStep(int[] steps) {
		this.chunkStep = steps;
		this.size = 1;
		for (int i = 0; i < this.chunkStep.length; ++i)
			this.size *= this.chunkStep[i];
	}

	public void setInChunk(int inChunk) {
		this.inChunk = inChunk;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public void setStart(int start, int i) {
		this.start[i] = start;
	}



	public void setStart(int[] start) {
		this.start = start;
	}

	public void setVsize(int[] vsize) {
		this.vsize = vsize;
	}

	public boolean ShapeEquals(DataChunk chunk) {

		for (int i = 0; i < this.chunkStep.length; i++) {
			if (chunk.getChunkStep()[i] != this.chunkStep[i])
				return false;
		}
		return true;
	}
	
	public int getChunkOffset()
	{
		if(this.offset != -1) return this.offset;
		int []starts = new int[start.length];
		int []vsizes = new int[vsize.length];
		int []cizes = new int[start.length];
		for(int i = 0 ; i < start.length;i++)
		{
			starts[i] = start[start.length - 1 - i];
			vsizes[i] = vsize[start.length - 1 - i];
			cizes[i] = this.getChunkSize()[start.length - 1 - i];

		}
		return getOffsetOfChunk(vsizes,cizes,starts);
	}
	static int getOffsetOfChunk(int []vsize, int[] csize, int []start)
	{
		int [] volume = new int [vsize.length];
		int []dsize = new int [vsize.length +1];
		dsize[vsize.length]=1;
		volume[0]=1;
		for(int i = 1; i < volume.length; i++)
		{
			volume[i] = volume[i-1]*vsize[i-1];
			
		}
		for(int i = vsize.length -1 ; i >= 0; i--)
		{
			dsize[i] = dsize[i+1] *( (vsize[i] - start[i]) > csize[i]? csize[i]:(vsize[i] - start[i]));
		}
		int offset = 0;
		for(int i = volume.length - 1; i >= 0 ; i--)
		{
			offset = offset + start[i] * volume[i]*dsize[i+1];
		}
		return offset;
	}
	
}
