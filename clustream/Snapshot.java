package clustream;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Snapshot {

	int order;
	long timeFrameId;
	long timestamp;
	Map<Integer, MicroCluster> clusters;
	
	public Snapshot(int order, long timeFrameId, long timestamp, Map<Integer, MicroCluster> clusters) {
		this.order = order;
		this.timeFrameId = timeFrameId;
		this.timestamp = timestamp;
		this.clusters = new HashMap<Integer, MicroCluster>();
        for (Map.Entry e : clusters.entrySet()) {
            int clusterId = (Integer) e.getKey();
            MicroCluster cluster = (MicroCluster) e.getValue();
            this.clusters.put(clusterId, new MicroCluster(cluster));
        }
	}

	public Map<Integer, MicroCluster> getClusters() {
        return clusters;
	}

	public long getTimestamp() {
		return timestamp;
	}

	// get the different clusters by subtracting a previous snapshot
	public Set<MicroCluster> getDiffClusters(Snapshot prevSnapshot) {
		Map<Integer, MicroCluster> beforeMap = prevSnapshot.getClusters();
		Map<Integer, MicroCluster> endMap = this.clusters;
		Set<MicroCluster> diffSet = new HashSet<MicroCluster>();
		for (MicroCluster originalCluster : endMap.values()) {
			// copy the original cluster to the base to be subtracted, so that the original cluster remains unchanged.
			MicroCluster base = new MicroCluster(originalCluster);
			if (base.isSingle()) {
				/* 1. if it is a single cluster, we have two cases:
				 * 1) beforeMap does not include this cluster, then this must be a new cluster, we keep all the elements in base.
				 * 2) beforeMap includes this cluster, then do the subtraction.
				 */
				if (beforeMap.containsKey(base.clusterID)) {
					MicroCluster before = beforeMap.get(base.clusterID);
					base.subtract(before);
				}
			} else {
				// 2. composite cluster
				Set<Integer> clusterIDSet = base.idSet;
				clusterIDSet.add(base.clusterID);
				for (Integer cid : clusterIDSet) {
					/* clusterIDSet have four cases:
					 * 1. beforeMap contains cid and it is a single cluster, then we can do subtraction directly
					 * 2. beforeMap contains cid and it is a composite cluster, we can also do subtraction directly
					 * 3. beforeMap does not include cid, and cid is a new cluster, no action is needed.
					 * 4. beforeMap does not include cid, but cid is in the idSet of some composite clusters, then
					 *    no action is needed, because it has already been processed in case 1.
					 */
					if (beforeMap.containsKey(cid)) {
						MicroCluster before = beforeMap.get(cid);
						base.subtract(before);
					}
				}
			}
			if (base.num > 0)
				diffSet.add(base);
		}
		return diffSet;
	}

	@Override
	public String toString() {
		String itemSep = "=";
		StringBuilder sb = new StringBuilder();
		sb.append(Long.toString(this.timestamp));
		sb.append(itemSep + Integer.toString(this.order));
		for (MicroCluster cluster : this.clusters.values())
			sb.append(itemSep + cluster.toString());
		return sb.toString();
	}

}

//	public static Snapshot string2Snapshot(String ss) {
//		String[] items = ss.split(itemSep);
//		long time = new Long(items[0]);
//		int order = new Integer(items[1]);
//		Map<Integer, TweetCluster> map = new HashMap<Integer, TweetCluster>();
//		for (int i = 2; i < items.length; i++) {
//			TweetCluster geoTweetCluster = TweetCluster.string2TCV(items[i]);
//			map.put(geoTweetCluster.clusterID, geoTweetCluster);
//		}
//		return new Snapshot(order, time, map);
//	}
