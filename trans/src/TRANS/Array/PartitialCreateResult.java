package TRANS.Array;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import TRANS.Data.TransDataType;
import TRANS.util.TRANSDataIterator;

public class PartitialCreateResult extends TRANSDataIterator {

	class PresultKey {
		int[] start = null;
		int[] shape = null;

		public PresultKey(int start[], int[] end) {
			this.start = start;
			this.shape = end;
		}
	}

	Set<PresultKey> dealtKeys = new HashSet<PresultKey>();

	public PartitialCreateResult(TransDataType type,Object[] ds, int[] start, int[] chunkSize) throws IOException {
		// TODO Auto-generated constructor stub
		super(type,ds,start,chunkSize);
	}

	public boolean AddResult(TRANSDataIterator itr) {
		boolean ret = true;
		PresultKey key = new PresultKey(itr.getStart(), itr.getShape());
		synchronized (dealtKeys) {
			if (this.dealtKeys.contains(key)) {
				ret = false;
			} else {
				this.dealtKeys.add(key);
			}
		}
		return ret;
	}
}
