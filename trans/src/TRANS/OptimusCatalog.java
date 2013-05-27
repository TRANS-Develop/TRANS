package TRANS;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.AlreadyBoundException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.hash.Hash;
import org.jdom2.JDOMException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import TRANS.Array.ArrayID;
import TRANS.Array.DataChunk;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusArray.ARRAY_STATUS;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusShapes;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Array.ZoneID;
import TRANS.Data.TransDataType;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.Protocol.OptimusDataProtocol;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusDefault;
import TRANS.util.TRANSWritableArray;
import TRANS.util.TransHostList;

public class OptimusCatalog extends Thread implements OptimusCatalogProtocol,
		Writable {
	static class CatalogRedoLogger {
		public enum OPERATION {
			CREATE_ZONE, DELETE_ZONE, CREATE_ARRAY, FINISH_ARRAY, DELETE_ARRAY
		}

		OptimusCatalog catalog = null;;

		private String logPath = null;
		private DataOutputStream out = null;
		
		public CatalogRedoLogger(OptimusCatalog cata, String logPath)
				throws FileNotFoundException {
			this.catalog = cata;
			this.logPath = logPath;
			File f = new File(logPath);
			if (!f.isDirectory()) {
				f.mkdir();
			}
			FileOutputStream fout = new FileOutputStream(logPath + "/"
					+ System.currentTimeMillis());
			out = new DataOutputStream(fout);
		}

		synchronized public boolean logOperation(TransOperation op) {
			if (catalog.recoverMode) {
				return true;
			}
			try {
				op.write(out);
				out.flush();
			} catch (IOException e) {

				e.printStackTrace();
				return false;
			}
			return true;
		}

	}

	class OptimusTimeRunner extends TimerTask {
		private OptimusCatalog catalog = null;

		public OptimusTimeRunner(OptimusCatalog catalog) {
			this.catalog = catalog;
		}

		public void check() throws IOException, WrongArgumentException {
			Vector<ArrayID> arrays = catalog.checkDeadArray();
			synchronized (catalog.deadArray) {
				catalog.deadArray.addAll(arrays);
			}
			Vector<Host> hs = catalog.checkDataNode();
			if (!hs.isEmpty()) {
				catalog.recover(hs);
			}

		}

		@Override
		public void run() {
			try {
				check();
			} catch (Exception e) {

				e.printStackTrace();
			}
		}

	}

	class webServer extends Thread {

		@Override
		public void run() {

			ServerSocket socket = null;
			try {
				socket = new ServerSocket(conf.getInt(
						"Optimus.catalog.webport", 7700));
			} catch (IOException e) {

				e.printStackTrace();
			}
			while (true) {
				Socket sin;
				try {
					sin = socket.accept();
					DataOutputStream cout = new DataOutputStream(
							sin.getOutputStream());
					cout.write(new String("Hello World!").getBytes());
				} catch (IOException e) {

					e.printStackTrace();
				}

			}
		}
	}

	/**
	 * @param args
	 * @throws AlreadyBoundException
	 * @throws IOException
	 * @throws ParseException
	 * @throws JDOMException
	 * @throws Exception
	 */
	public static void main(String[] args) throws AlreadyBoundException,
			IOException, ParseException, JDOMException, Exception {

		OptimusCatalog cl = null;
		System.out.println("Starting Catalog Node...");
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("c", true, "Configuration directory");
		CommandLine cmd = parser.parse(options, args);
		String confDir = cmd.getOptionValue("c");

		OptimusConfiguration conf = null;
		if (confDir != null) {
			conf = new OptimusConfiguration(confDir);
		} else {
			conf = new OptimusConfiguration("./conf");
		}

		cl = new OptimusCatalog(conf);
		cl.start();
		Runtime.getRuntime().addShutdownHook(new OptimusCatalogClear(cl));
		cl.join();

	}

	OptimusInstanceID nextHostId() {
		System.out.println("New Id:" + ids);
		return new OptimusInstanceID(ids++);
	}

	protected boolean recoverMode = true;

	private long checkPointTime = -1;
	private CatalogRedoLogger logger = null;
	private int ids = 0;
	private Set<ArrayID> deadArray = new HashSet<ArrayID>();
	private ConcurrentMap<ArrayID, ArrayCreateStatus> inCreateArray = new ConcurrentHashMap<ArrayID, ArrayCreateStatus>();
	private ConcurrentMap<ZoneID, ConcurrentHashMap<String, ArrayID>> arrayName = new ConcurrentHashMap<ZoneID, ConcurrentHashMap<String, ArrayID>>();
	private ConcurrentMap<ArrayID, OptimusArray> arrays = new ConcurrentHashMap<ArrayID, OptimusArray>();

	private OptimusConfiguration conf = null;
	private ConcurrentHashMap<OptimusInstanceID, OptimusPartitionStatus> hostStatus = new ConcurrentHashMap<OptimusInstanceID, OptimusPartitionStatus>();
	private ConcurrentHashMap<OptimusInstanceID,HashSet<Partition>> hostPartitions= new ConcurrentHashMap<OptimusInstanceID,HashSet<Partition>>();
	DataOutputStream metaOut = null; //

	private ConcurrentHashMap<OptimusInstanceID, Host> nodes = new ConcurrentHashMap<OptimusInstanceID, Host>();
	private ConcurrentHashMap<OptimusInstanceID, Host> liveNodes = new ConcurrentHashMap<OptimusInstanceID, Host>();
	// private ConcurrentSkipListSet<Host> deadNodes = new
	// ConcurrentSkipListSet<Host>();

	protected ConcurrentMap<ArrayID, ConcurrentHashMap<PID, Host[]>> partitions = new ConcurrentHashMap<ArrayID, ConcurrentHashMap<PID, Host[]>>();

	private int arrayNumber = 0;
	private int zoneNumber = 0;
	//threads
	private PartitionRecover recover = null;
	private webServer wserver = null;
	private Server server = null;

	private ConcurrentHashMap<String, ZoneID> zoneName = new java.util.concurrent.ConcurrentHashMap<String, ZoneID>();

	private ConcurrentHashMap<ZoneID, OptimusZone> zones = new java.util.concurrent.ConcurrentHashMap<ZoneID, OptimusZone>();

	OptimusCatalog(OptimusConfiguration conf) throws IOException {
		this.conf = conf;
		String name = this.conf.getString("Optimus.meta.path",
				OptimusDefault.META_PATH);
		String filename = this.conf.getString("Optimus.catalog.filename",
				OptimusDefault.META_FILENAME);
		File f = new File(name);
		if (!f.isDirectory()) {
			f.mkdir();
		}
		String path = name + "/" + filename;
		System.out.println("Entering Recover Mode!!");
		try {

			DataInputStream in = new DataInputStream(new FileInputStream(path));
			this.readFields(in);
			in.close();

		} catch (Exception e) {
			System.out.println("No meta file found");
		}
		boolean updated = this.RecoverLog(this.conf.getString(
				"Optimus.meta.path", OptimusDefault.META_PATH) + "/log/");
		if (updated) {
			File old = new File(path);
			File nf = new File(path + "_" + this.checkPointTime);
			old.renameTo(nf);
			DataOutputStream out = new DataOutputStream(new FileOutputStream(
					path));
			this.checkPointTime = System.currentTimeMillis();
			this.write(out);
			out.flush();
		}
		this.recoverMode = false;
		System.out.println("Finish Recover Mode!!");
		server = RPC.getServer(this, conf.getString("Optimus.catalog.host",
				OptimusDefault.CATALOG_HOST), conf.getInt(
				"Optimus.catalog.port", OptimusDefault.CATALOG_PORT), 10,
				false, new Configuration());

		logger = new CatalogRedoLogger(this, name + "/log/");
		Timer t = new Timer();
		t.schedule(new OptimusTimeRunner(this), 0,
				this.conf.getInt("Optimus.hearbeat.time",
						OptimusDefault.HEARTBEAT_TIME) * 1000);

	}

	public Vector<Host> checkDataNode() {
		System.out.println("Chenking Dead node");
		Vector<Host> deadHosts = new Vector<Host>();

		long deadTime = this.conf.getInt("Optimus.hearbeat.time",
				OptimusDefault.HEARTBEAT_TIME)  * 1000 * 3;

		 Set<Entry<OptimusInstanceID, Host>> set = this.liveNodes.entrySet();
		for (Entry<OptimusInstanceID, Host>  en : set) {
			
			Host h = (Host) en.getValue();
			long t = System.currentTimeMillis();
			System.out.println(t + ":" + h.getLiveTime() + "="
					+ (t - h.getLiveTime()) + "?" + deadTime);
			if (t - h.getLiveTime() > deadTime) {

				h.setDead(true);
				this.liveNodes.remove(h.getInstanceId());
				
				deadHosts.add(h);
			}
		}
		return deadHosts;
	}

	public Vector<ArrayID> checkDeadArray() {
		System.out.println("Checking Dead array");
		Vector<ArrayID> deadArray = new Vector<ArrayID>();

		long deadTime = this.conf.getInt("Optimus.hearbeat.time",
				OptimusDefault.HEARTBEAT_TIME) * 1000 * 3;
		for (Entry<ArrayID, ArrayCreateStatus> e : this.inCreateArray
				.entrySet()) {

			ArrayCreateStatus acs = e.getValue();
			if (acs.getLastUpdateStatus() - System.currentTimeMillis() > deadTime) {
				deadArray.add(e.getKey());
				this.inCreateArray.remove(e.getKey());
				OptimusArray array = this.arrays.get(e.getKey());
				synchronized (array) {
					array.setDeleted(ARRAY_STATUS.FAILED);
				}
			}
		}
		return deadArray;
	}

	public void close() throws IOException {
		System.out.println("Closing catalog");
		String name = this.conf.getString("Optimus.meta.path",
				OptimusDefault.META_PATH);
		String filename = this.conf.getString("Optimus.catalog.filename",
				OptimusDefault.META_FILENAME);
		String path = name + "/" + filename;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		this.write(out);
		out.close();
		
	}

	private ArrayID createArray(ArrayID aid, ZoneID zid, Text name,
			FloatWritable devalue,TransDataType type) throws WrongArgumentException {
		ConcurrentHashMap<String, ArrayID> aName = this.arrayName.get(zid);
		OptimusArray array = new OptimusArray(zid, aid, new String(
				name.getBytes()), devalue.get(),type);

		array.setDeleted(OptimusArray.ARRAY_STATUS.CREATING);
		this.arrays.put(aid, array);
		aName.put(new String(name.getBytes()), aid);
		ArrayCreateStatus acs = new ArrayCreateStatus();
		this.inCreateArray.put(aid, acs);
		OptimusZone zone = this.openZone(zid);
		int[] size = zone.getSize().getShape();
		long l = 1;
		for (int i = 0; i < size.length; i++) {
			l *= size[i];
		}
		acs.setTotalSize(l);
		acs.setLastUpdateStatus(System.currentTimeMillis());

		Vector<Writable> args = new Vector<Writable>();
		args.add(aid);
		args.add(zid);
		args.add(name);
		args.add(devalue);
		args.add(type);
		TransOperation op = new TransOperation(TRANSOP.CREATE_ARRAY, args);
		this.logger.logOperation(op);
		return aid;
	}

	@Override
	public ArrayID createArray(ZoneID zid, Text name, FloatWritable devalue,TransDataType type)
			throws TRANS.Exceptions.WrongArgumentException {
		ConcurrentHashMap<String, ArrayID> aName = this.arrayName.get(zid);
		if (aName == null) {
			throw new TRANS.Exceptions.WrongArgumentException("zid",
					"Not found response zone");
		}

		ArrayID aid = aName.get(new String(name.getBytes()));
		if (aid != null) {
			throw new TRANS.Exceptions.WrongArgumentException("name",
					"Existing array for zone:" + zid);
		}

		aid = new ArrayID(this.getNextArrayID());
		this.createArray(aid, zid, name, devalue,type);
		return aid;
	}

	@Override
	public BooleanWritable CreatePartition(Partition p)
			throws WrongArgumentException {
		OptimusZone zone = this.openZone(p.getZid());
		Set<Host> hosts = new HashSet<Host>();
		int len = this.liveNodes.size();
		int rsize = zone.getReplicaNumber();
		
		if (rsize > this.liveNodes.size()) {
			throw new WrongArgumentException("Replica number",
					"No enough node to serve the replicas");
		}
		Random random = new Random(System.currentTimeMillis());
		int ret = 0;
		Host[] hostid = new Host[rsize];
		for (int i = 0; i < rsize;) {
			ret = random.nextInt();
			ret = ret > 0 ? ret : 0-ret;
			OptimusInstanceID id = new OptimusInstanceID(ret % len);
			System.out.println(ret);
			Host h = this.liveNodes.get(id);
			if (hosts.contains(h) || h.isDead()) {
				continue;
			}
			hosts.add(h);
			hostid[i++] = h;

		}
		ConcurrentHashMap<PID, Host[]> ph = null;
		synchronized (this.partitions) {
			ph = this.partitions.get(p.getArrayid());
			if (ph == null) {
				ph = new ConcurrentHashMap<PID, Host[]>();
				this.partitions.put(p.getArrayid(), ph);
			}else{
				//created partition
				return new BooleanWritable(true);
			}
		}
		int num = 0;
		for(Host h: hostid)
		{
			HashSet<Partition> ps = null;
			synchronized(this.hostPartitions)
			{
				ps = this.hostPartitions.get(h.getInstanceId());
				if(ps == null)
				{
					ps = new HashSet<Partition>();
					this.hostPartitions.put(h.getInstanceId(), ps);
				}
			}
			synchronized(ps)
			{
				
				Partition np = new Partition(p.getZid(),p.getArrayid(),p.getPid(),p.getRid());
				np.setRid(new RID(num++));
				System.out.println("Add partition"+np+" for host"+h);
				ps.add(np);
			}
		}
		ph.put(p.getPid(), hostid);
		return new BooleanWritable(true);
	}

	@Override
	public OptimusZone createZone(Text name, OptimusShape size,
			OptimusShape step, OptimusShapes strategy)
			throws WrongArgumentException {
		ZoneID id = null;
		if ((id = this.zoneName.get(new String(name.getBytes()))) != null) {
			throw new WrongArgumentException("zone name", "Exsiting Zone");
		}
		if( (strategy.getShapes().size() - 1) > this.liveNodes.size())
		{
			throw new WrongArgumentException("zone replica number", "Not enough nodes to create replicas"+(strategy.getShapes().size() - 1)+":"+this.liveNodes.size());
		}
		id = new ZoneID(this.getNextZoneID());
		System.out.println("Creating Zone" + id);
		OptimusZone zone = new OptimusZone(new String(name.getBytes()), id,
				size, step, strategy);

		this.zoneName.put(new String(name.getBytes()), id);
		this.zones.put(id, zone);
		this.arrayName.put(id, new ConcurrentHashMap<String, ArrayID>());
		if (!this.recoverMode) {
			Vector<Writable> args = new Vector<Writable>();
			args.add(name);
			args.add(size);
			args.add(step);
			args.add(strategy);
			TransOperation op = new TransOperation(TRANSOP.CREATE_ZONE, args);
			this.logger.logOperation(op);
		}
		return zone;
	}

	@Override
	public BooleanWritable deleteArray(ArrayID aid) {
		OptimusArray array = this.arrays.get(aid);
		array.setDeleted(ARRAY_STATUS.DELETED);
		if (this.inCreateArray.containsKey(aid)) {
			this.inCreateArray.remove(aid);
		}
		ConcurrentHashMap<String, ArrayID> map = this.arrayName.get(array
				.getZid());
		map.remove(array.getName());
		this.deadArray.add(aid);
		this.arrays.remove(aid);

		Vector<Writable> args = new Vector<Writable>();
		args.add(aid);

		TransOperation op = new TransOperation(TRANSOP.DELETE_ARRAY, args);
		this.logger.logOperation(op);
		return new BooleanWritable(true);
	}

	@Override
	public BooleanWritable deleteZone(ZoneID id) throws WrongArgumentException {
		ConcurrentHashMap<String, ArrayID> zarrays = this.arrayName.get(id);
		if (zarrays == null) {
			throw new WrongArgumentException("Zone", id + "Not Exists");
		} else if (!zarrays.isEmpty()) {
			throw new WrongArgumentException("Zone", id + "Not empty");
		}
		this.arrayName.remove(id);
		OptimusZone zone = this.zones.get(id);
		this.zones.remove(id);
		this.zoneName.remove(zone.getId());
		Vector<Writable> args = new Vector<Writable>();
		args.add(id);

		TransOperation op = new TransOperation(TRANSOP.DELETE_ZONE, args);
		this.logger.logOperation(op);
		return new BooleanWritable(true);
	}

	@Override
	public TransHostList getHosts(Partition p) throws WrongArgumentException {

		ZoneID z = p.getZid();
		OptimusZone zone = this.openZone(z);
		int len = zone.getStrategy().getShapes().size() - 1;
		TransHostList list = new TransHostList();
		for (int i = 0; i < len; i++) {
			Host h = this.getReplicateHost(p, new RID(i));
			list.appendHost(h, 0.0);
		}
		return list;
	}

	protected Host getLiveNode(Partition p) throws WrongArgumentException {

		OptimusZone z = this.zones.get(p.getZid());
		int r = z.getReplicaNumber();
		int l;
		Random rd = new Random(System.currentTimeMillis());
		do{
			l = rd.nextInt();
			l = l > 0 ? l : 0- l;
			l%=r;
		}while(l == p.getRid().getId());
		
		return this.getReplicateHost(p, new RID(l));
	}

	private Host getNewNodeForPartition(Partition p) {
		try {
			TransHostList list = this.getHosts(p);
			Vector<Host> hs = list.getHosts();
			int ret = -1;
			int len = this.nodes.size();
			Random random = new Random(System.currentTimeMillis());
			while (true) {
				ret = random.nextInt();
				ret = ret > 0 ? ret : 0 - ret;
				
				OptimusInstanceID id = new OptimusInstanceID(ret % len);
				Host h = this.liveNodes.get(id);
				if (h == null || hs.contains(h) ) {
					continue;
				}
				return h;
			}
		} catch (WrongArgumentException e) {
			return null;
		}
	}

	private Host getNewNodeForZone(ZoneID id) {
		/*
		 * Host host = null; while(true) { // host = this.liveNodes.; if( !
		 * host.isDead() ) { break; } }
		 */
		// return host;
		return null;
	}

	synchronized private int getNextArrayID() {
		return this.arrayNumber++;
	}

	synchronized private int getNextZoneID() {
		return this.zoneNumber++;
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {

		return 1;
	}

	@Override
	public Host getReplicateHost(Partition p, RID rid)
			throws WrongArgumentException {
		/*
		 * int n = p.getPid().getId() + rid.getId() + p.getZid().getId(); n %=
		 * this.nodes.size(); return this.nodes.get(new OptimusInstanceID(n));
		 */
		ConcurrentHashMap<PID, Host[]> ph = this.partitions.get(p.getArrayid());
		if (ph == null) {
			throw new WrongArgumentException("partition mismatch array",
					"Unknow partition");
		}
		Host[] h = ph.get(p.getPid());
		if (h == null) {
			throw new WrongArgumentException("partition mismatch partition",
					"Unknow partition");
		}
		if (rid.getId() >= h.length) {
			throw new WrongArgumentException("partition mismatch partition",
					"Wrong Replica ID");
		}
		System.out.println("Get host for "+rid +Arrays.toString(h));
		return h[rid.getId()];
	}

	@Override
	public OptimusPartitionStatus heartBeat(Host host, OptimusPartitionStatus status) {
		System.out.println("Heart Beat from Node:" + host.toString());
		OptimusInstanceID oid = host.getInstanceId();
		if (oid.getId() < 0) {
			oid = this.Register(host);
		} else {
			Host h = this.nodes.get(host.getInstanceId());
			if(h.equals(host))
			{
				this.nodes.get(oid).update();
				this.liveNodes.put(host.getInstanceId(), host);
				host.setDead(false);
				System.out.println(this.nodes.size());
				System.out.println(this.liveNodes.size());
			
			}else{
				System.out.println("System Conflict Host is refused!!"+host);
				return new OptimusPartitionStatus();
			}
		}
		Vector<Partition> todelete = new Vector<Partition>();
		Host uhost = this.nodes.get(oid);
		//Set<ArrayID> todelete = new HashSet<ArrayID>();
		while (status.hasNext()) {
			Partition tmp = status.nexPartition();
			System.out.println(tmp);
			ArrayID aid = tmp.getArrayid();
			OptimusArray array = this.arrays.get(aid);
			OptimusZone zone = this.zones.get(tmp.getZid());
			if (this.deadArray.contains(aid) || zone == null || array == null) {
				System.out.println("Reporting deleted array partitions");
				//todelete.add(aid);
				todelete.add(tmp);
				continue;
			}

			ConcurrentHashMap<PID, Host[]> phost = this.partitions.get(tmp
					.getArrayid());
			if (phost == null) {
				phost = new ConcurrentHashMap<PID, Host[]>();
				this.partitions.put(tmp.getArrayid(), phost);
			}
			Host[] h = phost.get(tmp.getPid());
			if (h == null) {
				h = new Host[this.zones.get(tmp.getZid()).getStrategy()
						.getShapes().size()];
				phost.put(tmp.getPid(), h);
			}
			// h.set(tmp.getRid().getId(), host);
			int o = tmp.getRid().getId();
			Host oh = h[o];
			if(oh== null || oh.equals(uhost)|| oh.isDead())
			{
				System.out.println(oh+"Conflict with"+uhost);
				h[tmp.getRid().getId()] = uhost;
			}else{
				todelete.add(tmp);
			}

		}

		return new OptimusPartitionStatus(todelete);
	}

	@Override
	public void interrupt() {
		try {
			
			this.close();
		} catch (IOException e) {

			e.printStackTrace();
		}
		this.wserver.interrupt();
		this.recover.interrupt();
		super.interrupt();
	}

	@Override
	public OptimusArray openArray(ArrayID aid) throws WrongArgumentException {

		OptimusArray arry = this.arrays.get(aid);
		if (arry == null) {
			throw new TRANS.Exceptions.WrongArgumentException(
					"Unknown ArrayId ", aid.toString());
		}
		return arry;
	}

	@Override
	public OptimusArray openArray(ZoneID zid, Text name)
			throws TRANS.Exceptions.WrongArgumentException {

		ConcurrentHashMap<String, ArrayID> aName = this.arrayName.get(zid);
		if (aName == null) {
			throw new TRANS.Exceptions.WrongArgumentException("zid",
					"Not found response zone");
		}

		ArrayID aid = aName.get(new String(name.getBytes()));
		if (aid == null) {
			throw new TRANS.Exceptions.WrongArgumentException("ArrayName "
					+ name, "Wrong array name?");
		}
		OptimusArray array = this.arrays.get(aid);
		if (array == null) {
			throw new TRANS.Exceptions.WrongArgumentException("ArrayName",
					"Unknown internal error");
		}
		return array;
	}

	@Override
	public OptimusZone openZone(Text name) throws WrongArgumentException {

		ZoneID id = this.zoneName.get(new String(name.getBytes()));
		if (id == null) {
			throw new WrongArgumentException("Zonename", "Unknown Zone" + name);
		}
		return this.zones.get(id);
	}

	@Override
	public OptimusZone openZone(ZoneID id) throws WrongArgumentException {

		OptimusZone zone = null;
		if ((zone = this.zones.get(id)) == null) {
			throw new WrongArgumentException("Zid" + id, "unknown zone");
		}
		return zone;

	}

	@Override
	public void readFields(DataInput in) throws IOException {

		// arrays
		synchronized (this) {
			this.checkPointTime = in.readLong();
			this.arrays.clear();
			this.arrayName.clear();
			this.arrayNumber = in.readInt();
			this.zoneNumber = in.readInt();
			this.ids = in.readInt();
			int size = in.readInt();
			for (int i = 0; i < size; i++) {
				OptimusArray a = new OptimusArray();
				a.readFields(in);
				this.arrays.put(a.getId(), a);
				ConcurrentHashMap<String, ArrayID> znames = this.arrayName
						.get(a.getZid());
				if (znames == null) {
					znames = new ConcurrentHashMap<String, ArrayID>();
					this.arrayName.put(a.getZid(), znames);
				}
				znames.put(a.getName(), a.getId());
			}

			// zones
			size = in.readInt();
			for (int i = 0; i < size; i++) {
				OptimusZone zone = new OptimusZone();
				zone.readFields(in);
				this.zones.put(zone.getId(), zone);
				this.zoneName.put(zone.getName(), zone.getId());
			}
		}
	}

	private void recover(Host host) throws IOException, WrongArgumentException {
		System.out.println("Recovying " + host);
		OptimusPartitionStatus status = this.hostStatus.get(host
				.getInstanceId());
		if (status == null)
		{
			System.out.println("No Partition for "+host);
			return;
		}
		Set<Partition> ps = this.hostPartitions.get(host.getInstanceId());
		if(ps == null)
		{
			return;
		}
		for (Partition p : ps) {
			System.out.println("Recoverying " + p);
			// Host dph = this.getNewNodeForZone(p.getZid());
			/*
			Host dph = getNewNodeForPartition(p);
			Host ho = this.getLiveNode(p);
			OptimusDataProtocol dp = dph.getDataProtocol();
			IntWritable ret = dp.RecoverPartition(p, ho);
			if (ret == null
					|| ret.get() == OptimusReplicationManager.REPLICATE_FAILURE) {
				System.out.println("Recover partition failure");
				continue;
			} else {
				System.out.println("The data of " + p + "is moving to" + dph);
				ConcurrentHashMap<PID, Host[]> hs = this.partitions.get(p
						.getArrayid());
				Host[] hh = hs.get(p.getPid());
				synchronized (hh) {
					hh[p.getRid().getId()] = dph;
				}
			}*/
			recover.addLost(p);
			synchronized(ps){
				ps.remove(p);
			}
		}
	}

	public void recover(Vector<Host> hosts) throws IOException,
			WrongArgumentException {
		for (Host host : hosts) {
			this.recover(host);
		}
	}

	private boolean RecoverLog(String logPath) throws IOException {

		boolean updated = false;
		File f = new File(logPath);

		File[] files = f.listFiles();
		if (files == null) {
			return false;
		}
		for (File l : files) {
			long logTime = Long.MAX_VALUE;
			try {
				logTime = Long.parseLong(l.getName());
			} catch (Exception e) {
				continue;
			}
			if (this.checkPointTime > logTime) {
				continue;
			}

			FileInputStream fs = new FileInputStream(l.getAbsoluteFile());
			DataInputStream din = new DataInputStream(fs);
			try {
				int op = -1;
				do {
					TRANSOP ops = null;
					op = din.readInt();
					for (TRANSOP o : TRANSOP.values()) {
						if (o.ordinal() == op) {
							ops = o;
							break;
						}
					}
					if (ops == null) {
						din.close();
						continue;
					}
					updated = true;
					if (ops.equals(TRANSOP.CREATE_ARRAY)) {
						ArrayID aid = new ArrayID();
						aid.readFields(din);
						ZoneID id = new ZoneID();
						id.readFields(din);
						Text name = new Text();
						name.readFields(din);
						FloatWritable dvalue = new FloatWritable();
						dvalue.readFields(din);
						TransDataType t = new TransDataType();
						t.readFields(din);
						this.createArray(aid, id, name, dvalue,t);
					} else if (ops.equals(TRANSOP.CREATE_ZONE)) {
						Text name = new Text();
						name.readFields(din);
						OptimusShape size = new OptimusShape();
						OptimusShape step = new OptimusShape();
						OptimusShapes strategy = new OptimusShapes();
						size.readFields(din);
						step.readFields(din);
						strategy.readFields(din);
						this.createZone(name, size, step, strategy);

					} else if (ops.equals(TRANSOP.DELETE_ZONE)) {
						ZoneID zid = new ZoneID();
						zid.readFields(din);
						this.deleteZone(zid);
						System.out.println("LOG: DELETE Zone " + zid);
					} else if (ops.equals(TRANSOP.DELETE_ARRAY)) {
						ArrayID aid = new ArrayID();
						aid.readFields(din);
						this.deleteArray(aid);
						System.out.println("LOG: DELETE_ARRAY");
					} else if (ops.equals(TRANSOP.FINISH_ARRAY)) {
						System.out.println("LOG: FINISH_ARRAY");
					} else {
						System.out.println("UNKNOW OPERATION");
					}

				} while (op >= 0);
			} catch (Exception e) {
				din.close();
				continue;
			}
		}
		return updated;
	}

	@Override
	public OptimusInstanceID Register(Host host) {

		System.out.println("Register host:" + host);
		if (host.getInstanceId().getId() >= 0) {
			Host h = this.nodes.get(host.getInstanceId());
			if (h == null) {
				this.nodes.put(host.getInstanceId(), host);
				return host.getInstanceId();
			} else if (h.equals(host)) {
				return host.getInstanceId();
			} else {
				System.out.println("Log Error:+conflict host" + host + h);
				return new OptimusInstanceID(-1);
			}
		} else {

			host.setInstanceId(this.nextHostId());
			this.nodes.put(host.getInstanceId(), host);
		}

		return host.getInstanceId();
	}

	public void start() {
		wserver = new webServer();
		wserver.start();
		recover = new  PartitionRecover(this,1);
		recover.start();
		server.start();
	}

	@Override
	public BooleanWritable stopCatalog() {
		boolean stopOk = true;
		try {
			this.close();
		} catch (IOException e) {

			e.printStackTrace();
			stopOk = false;
		}
		return new BooleanWritable(stopOk);
	}

	@Override
	public BooleanWritable updateArrayStatus(ArrayID aid,
			long uploadedSinceLastReport) {
		ArrayCreateStatus acs = this.inCreateArray.get(aid);
		if (acs == null) {
			System.out.println("Log: update of the status of wrong array");
			return new BooleanWritable(false);
		}
		synchronized (acs) {
			long t = System.currentTimeMillis();
			acs.setCreatingSpeed((uploadedSinceLastReport * 1000)
					/ (1024 * 1024 * (1.0) * (t - acs.getLastUpdateStatus())));
			acs.setLastUpdateStatus(t);
			acs.setCreatedSize(acs.getCreatedSize() + uploadedSinceLastReport);
			if (acs.isFull()) {
				this.inCreateArray.remove(aid);
			}
			OptimusArray array = this.arrays.get(aid);
			array.setDeleted(ARRAY_STATUS.FINISHED);
		}
		return new BooleanWritable(true);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		// arrays
		synchronized (this) {
			out.writeLong(this.checkPointTime);
			out.writeInt(this.arrayNumber);
			out.writeInt(this.zoneNumber);
			out.writeInt(this.ids);
			
			Set<Entry<ArrayID, OptimusArray>> s = this.arrays.entrySet();

			out.writeInt(s.size());
			OptimusArray a = null;

			for (Entry<ArrayID, OptimusArray> e : s) {
				a = (OptimusArray) e.getValue();
				a.write(out);
			}

			// zones
			Set<Entry<ZoneID, OptimusZone>> sz = this.zones.entrySet();
			OptimusZone z = null;
			out.writeInt(sz.size());

			for (Entry<ZoneID, OptimusZone> e : sz) {
				z = (OptimusZone) e.getValue();
				z.write(out);
			}
		}
	}

	class PartitionRecover extends Thread {
		private Semaphore semp = new Semaphore(0);
		private int tnum = 0;// number of thread to use
		private Queue<Partition> lostPartitions = new LinkedList<Partition>();
		private OptimusCatalog catalog = null;

		public PartitionRecover(OptimusCatalog catalog,int num) {
			this.catalog = catalog;
			tnum = num;
		}

		public void addLost(Partition p) {
			System.out.println(p+"is Adding to the recovery  queue");
			synchronized (lostPartitions) {
				lostPartitions.add(p);
				semp.release();
			}
		}

		public boolean recover(Partition p) throws WrongArgumentException,
				IOException {
			System.out.println("Begin the recovery of" + p);
			OptimusZone zone = catalog.openZone(p.getZid());
			
			/*
			 * 
			 */
			DataChunk chunk = new DataChunk(zone.getSize().getShape(),zone.getPstep().getShape());
			DataChunk tmpar = null;
			int pnum = p.getPid().getId();
			for (int i = 0; i < zone.getSize().getShape().length; i++) {
				
				while (chunk != null && chunk.getChunkNum() < pnum) {
					tmpar = chunk;
					chunk = chunk.moveUp(i);
					
				}
				if(chunk == null) chunk = tmpar;
				if (chunk.getChunkNum() == pnum) {
					break;
				}
				chunk = tmpar;
				
			}
			
			Host  ho= getNewNodeForPartition(p);
			Host dph = catalog.getLiveNode(p);
			OptimusDataProtocol dp = dph.getDataProtocol();
			System.out.println("Recovery from "+ho);
			IntWritable ret = dp.RecoverPartition(p,new OptimusShape(chunk.getChunkSize()), ho);
			if (ret == null
					|| ret.get() == OptimusReplicationManager.REPLICATE_FAILURE) {
				System.out.println("Recover partition failure"+ret);
				this.addLost(p);// add to the tail of the recover process
				return false;
			} else {
				System.out.println("The data of " + p + "is moving to" + dph);
				ConcurrentHashMap<PID, Host[]> hs = partitions.get(p
						.getArrayid());
				Host[] hh = hs.get(p.getPid());
				synchronized (hh) {
					hh[p.getRid().getId()] = dph;
				}
			}
			return true;
		}

		public void run() {
			System.out.println("The recovery process is started");
			while (true) {
				Partition p = null;
				try {
					this.semp.acquire();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					continue;
				}
				synchronized (lostPartitions) {
					p = this.lostPartitions.poll();
				}
				try {
					this.recover(p);
				} catch (WrongArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					synchronized (lostPartitions) {
						lostPartitions.add(p);
					}
				}
			}
		}
	}

	@Override
	public Text ListZone(Text name){
		JSONObject obj = new JSONObject ();
		ZoneID zid = this.zoneName.get(name.toString());
		if(zid == null)
		{
			obj.put("code", 1);
		}
		JSONArray arrayName = new JSONArray();
		JSONArray arrayID = new JSONArray();
		ConcurrentHashMap<String, ArrayID> as = this.arrayName.get(zid);
		
		for(Entry<String,ArrayID> e: as.entrySet())
		{
			arrayName.add(e.getKey());
			arrayID.add(e.getValue().getArrayId());
		}
		obj.put("arrayName", arrayName);
		obj.put("arrayID", arrayID);
		return new Text(obj.toJSONString());
	}

	@Override
	public Text listArray(ArrayID aid) {
		// TODO Auto-generated method stub
		return null;
	}
}
