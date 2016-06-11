package clustream;

import java.util.*;

public class Pyramid {

    // parameters
	private int alpha = 2;
    private int ll = 3;
	private int capacity;
	// order: timestamps
    Map<Integer, LinkedList<Long>> orderMap = new HashMap<Integer, LinkedList<Long>>();
    Map<Long, Snapshot> snapshots = new HashMap<Long, Snapshot>();
    // time frame granularity (hour) with respect to the general timestamp (second)
    int timeFrameGranularity = 3600;
    // the newest time frame number
    long currentTimeFrameId = -1;
    
	public Pyramid(int alpha, int ll, int timeFrameGranularity) {
		this.alpha = alpha;
        this.ll = ll;
		this.capacity = (int)Math.pow(this.alpha, ll) + 1;
        this.timeFrameGranularity = timeFrameGranularity;
	}

	public Pyramid() {
		this.capacity = (int)Math.pow(this.alpha, ll) + 1;
	}

    // number of snapshots in this pyramid
    public int size() {
        return snapshots.size();
    }

    public boolean isNewTimeFrame(long timestamp) {
        return (timestamp / timeFrameGranularity) != currentTimeFrameId;
    }

    public long toTimeFrameId(long timestamp) {
        return timestamp / timeFrameGranularity;
    }

	public void storeSnapshot(long timestamp, Map<Integer, MicroCluster> clusters) {
        long timeFrameId = toTimeFrameId(timestamp);
        currentTimeFrameId = timeFrameId;
        // compute which order the snapshot should go to
		int order = getOrder(timeFrameId);
        // store the snapshort into the corresponding order
		if (orderMap.containsKey(order)) {
            // appending to the list, the last one in the list is the newest snapshot.
			LinkedList<Long> list = orderMap.get(order);
            // If the time frame id is already contained in the pyramid, do nothing.
            for (Long existingId : list) {
                if (timeFrameId == existingId)
                    return;
            }
			list.addLast(timeFrameId);
			if (list.size() > capacity) {
                // when the layer is full, delete the oldest snapshot and free the memory
				Long removed = list.pollFirst();
				snapshots.remove(removed);
			}
		}
		else {
			LinkedList<Long> list = new LinkedList<Long>();
			list.add(timeFrameId);
			orderMap.put(order, list);
		}
        // write the snapshot (in memory)
		Snapshot snapshot = new Snapshot(order, timeFrameId, timestamp, clusters);
		snapshots.put(timeFrameId, snapshot);
	}


    // find the deepest order for the timeframeid
    private int getOrder(long timeFrameId) {
        if (timeFrameId == 0)
            return 0;
        int order = 0;
        int tmp = alpha;
        while (timeFrameId % tmp == 0) {
            order ++;
            tmp = (int) Math.pow(alpha, order + 1);
        }
        return order;
    }


    /************************** Functions for retrieving snapshot(s) **************************/
    public Snapshot loadSnapshot(long timeFrameId) {
        return snapshots.get(timeFrameId);
    }

    public Snapshot getSnapshotJustBefore(long timestamp) {
        long queryTimeFrameId = toTimeFrameId(timestamp);
        long timeFrameId = findTimeFrameJustBefore(queryTimeFrameId);
        return loadSnapshot(timeFrameId);
    }

    // get the snapshots between a range
    public List<Snapshot> getSnapshotsBetween(long startTimestamp, long endTimestamp) {
        long startTimeFrameId = toTimeFrameId(startTimestamp);
        long endTimeFrameId = toTimeFrameId(endTimestamp);
        List<Long> timeFrameIds = findTimeFrameBetween(startTimeFrameId, endTimeFrameId);
        List<Snapshot> results = new ArrayList<Snapshot> ();
        for (Long timeFrameId : timeFrameIds) {
            Snapshot snapshot = loadSnapshot(timeFrameId);
            if (snapshot == null) {
                System.out.println("Null snapshot! " + timeFrameId);
            }
            results.add(snapshot);
        }
        return results;
    }

    // find the snapshot for a given timestamp, if there is no exact match, return the nearest
    private long findTimeFrameJustBefore(long queryTimeFrameId) {
        int order = getOrder(queryTimeFrameId);
        // note that the snapshots are stored from old to new, namely ascending order of the queryTimeFrameId
        LinkedList<Long> list = this.orderMap.get(order);
        // if this is an exact match, return the snapshot
        if (list.contains(queryTimeFrameId))
            return queryTimeFrameId;
        // if no exact match, then find the nearest snapshot before this query queryTimeFrameId
        long mostRecent = -1;
        for (LinkedList<Long> sameOrderList : this.orderMap.values()) {
            for (int i = 0; i < sameOrderList.size(); i++) {
                long ts = sameOrderList.get(i);
                if (ts > mostRecent)
                    mostRecent = ts;
                if (ts > queryTimeFrameId)
                    break;
            }
        }
        // find the nearest snapshot.
        if (mostRecent == -1) {
            for (LinkedList<Long> sameOrderList : this.orderMap.values()) {
                long ts = sameOrderList.get(0);
                if (mostRecent == -1 || ts < mostRecent)
                    mostRecent = ts;
            }
        }
        return mostRecent;
    }

    // find the sorted timeFrameIds that fall in a range.
    private List<Long> findTimeFrameBetween(long startTimeFrameId, long endTimeFrameId) {
        List<Long> results = new ArrayList<Long> ();
        for (LinkedList<Long> sameOrderList : this.orderMap.values()) {
            for (int i = 0; i < sameOrderList.size(); i++) {
                long ts = sameOrderList.get(i);
                if (ts > endTimeFrameId)
                    break;
                if (ts >= startTimeFrameId && ts <= endTimeFrameId)
                    results.add(ts);
            }
        }
        Collections.sort(results);
        return results;
    }

    public String printStats() {
        String ret = "Order map: ";
        for (LinkedList<Long> sameOrderList : this.orderMap.values()) {
            ret += sameOrderList.toString() + "\n";
        }
        ret += "Snapshots: " + snapshots.keySet();
        return ret;
    }

}

