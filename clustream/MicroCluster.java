package clustream;

import geo.GeoTweet;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.*;

public class MicroCluster {
	
	// for the method getFreshness(); if the number of tweets is too small, just return the average time as freshness.
	static int MU_THRESHOLD = 50;

	int clusterID = -1;			// cluster id
    RealVector sum;	  // sum
	RealVector ssum;  // ssum
    int num = 0;				// number of tweets in the cluster
	long ts1 = 0;				// sum(ts)
	long ts2 = 0;				// sum(square(ts))
    Set<Integer> idSet = null;	// used only if this is a composite cluster
	Map<Integer, Integer> words = null;	// this map stores the semantic information, <keyword id, count>

	public MicroCluster(int id, RealVector sum, RealVector ssum, long ts1, long ts2, int num) {
		this.clusterID = id;
		this.sum = sum;
		this.ssum = ssum;
		this.ts1 = ts1;
		this.ts2 = ts2;
		this.num = num;
	}

    public MicroCluster(MicroCluster other) {
        this.clusterID = other.clusterID;
        this.num = other.num;
        this.sum = new ArrayRealVector(other.getSum());
        this.ssum = new ArrayRealVector(other.getSquareSum());
        this.ts1 = other.ts1;
        this.ts2 = other.ts2;
        if (other.idSet != null)
            this.idSet = new HashSet<Integer>(other.idSet);
        this.words = new HashMap<Integer, Integer>(other.words);
    }

    // Initialize a cluster with the given list of tweets and cluster id.
	public MicroCluster(List<GeoTweet> memberList, int id) {
		this.clusterID = id;
		// init num, ssum, sum
		this.num = 0;
		this.sum = new ArrayRealVector(2); // sum
		this.ssum = new ArrayRealVector(2); // ssum
		this.words = new HashMap<Integer, Integer>();
		for (GeoTweet tweet : memberList)
			absorb(tweet);
	}

    // whether this is a single cluster or not.
    public boolean isSingle() {
        return idSet == null;
    }

    public int size() {
        return num;
    }

    public int getId() {
        return clusterID;
    }

    public RealVector getSum() {
        return sum;
    }
    
    public RealVector getSquareSum() {
        return ssum;
    }

    public RealVector getCentroid() {
        return sum.mapDivide(num);
    }

	public Map<Integer, Integer> getWords() {
		return words;
	}

    public double getFreshness(double quantile) {
        double muTime = ts1 / num;
        // If there are too few tweets.
        if (num < MU_THRESHOLD)
            return muTime;
        double sigmaTime = Math.sqrt(ts2/num - (ts1/num)*(ts1/num));
        return muTime + sigmaTime * quantile;
    }

    // add one tweet
    public void absorb(GeoTweet tweet) {
        num ++;
        ts1 += tweet.getTimestamp();
        ts2 += tweet.getTimestamp() * tweet.getTimestamp();
        RealVector loc = tweet.getLocation().toRealVector();
        sum = sum.add(loc);
        ssum = ssum.add(loc.ebeMultiply(loc));
        for (Integer wordId : tweet.getEntityIds()) {
            int cnt = words.containsKey(wordId) ? words.get(wordId) : 0; // orginal count
            words.put(wordId, cnt+1);
        }
    }

    // merge other to this cluster
	public void merge(MicroCluster other) {
		// 1. update idList
		if (idSet == null)
			idSet = new HashSet<Integer>();
		idSet.add(other.clusterID);
        // If other is a composite cluster, then we need to incorporate the member ids.
		if (other.idSet != null) {
            idSet.addAll(other.idSet);
		}
		// 2. update num, ts1, ts2, sum, ssum
		num += other.num;
		ts1 += other.ts1;
		ts2 += other.ts2;
        sum = sum.add(other.getSum());
        ssum = ssum.add(other.getSquareSum());
		// 3. update the semantic information
		for (Map.Entry e : other.getWords().entrySet()) {
			int wordId = (Integer) e.getKey();
			int cnt = (Integer) e.getValue();
			int originalCnt = words.containsKey(wordId) ? words.get(wordId) : 0;
			words.put(wordId, originalCnt + cnt);
		}
	}

    public void subtract(MicroCluster other) {
        sum = sum.subtract(other.getSum());
        ssum = ssum.subtract(other.getSquareSum());
        ts1 -= other.ts1;
        ts2 -= other.ts2;
        num -= other.num;
        for (Map.Entry e : other.getWords().entrySet()) {
            int wordId = (Integer) e.getKey();
            int cnt = (Integer) e.getValue();
            int originalCnt = words.containsKey(wordId) ? words.get(wordId) : 0;
            if (originalCnt < cnt) {
                System.err.println("Original count is smaller than the new count!");
                System.exit(0);
            } else if (originalCnt == cnt) {
                words.remove(wordId);
            } else {
                words.put(wordId, originalCnt - cnt);
            }
        }
    }


    @Override
	public String toString() {
        String itemSep = "+";
		StringBuilder sb = new StringBuilder();
		sb.append(this.clusterID + itemSep);
		if (this.idSet == null)
			sb.append(0 + itemSep);
		else
			sb.append(1 + itemSep);
		sb.append(this.num + itemSep);
		sb.append(this.ts1 + itemSep);
		sb.append(this.ts2 + itemSep);
		sb.append(sum.toString() + itemSep);
		sb.append(ssum.toString() + itemSep);
		if (this.idSet != null) {
			for (Integer id : this.idSet)
				sb.append(id + " ");
		}
        sb.append(itemSep);
        sb.append(words.toString());
		return sb.toString();
	}

//    public static TweetCluster string2TCV(String ss) {
//        String itemSep = "+";
//        String[] items = ss.split("\\" + itemSep);	// '+' needs to be processed in regular expression
//        int id = new Integer(items[0]);
//        int type = new Integer(items[1]);
//        if ((type == 0 && items.length != 7) || (type == 1 && items.length != 8))
//            System.err.print("geoTweetCluster parse error: " + ss);
//        int num = new Integer(items[2]);
//        long ts1 = new Long(items[3]);
//        long ts2 = new Long(items[4]);
//        String [] sumItems = items[5].split(",");
//        String [] sSumItems = items[6].split(",");
//        double [] sumArray = new double[2];
//        double [] sSumArray = new double[2];
//        for (int i=0; i<2; i++) {
//            sumArray[i] = new Double(sumItems[i]).doubleValue();
//            sSumArray[i] = new Double(sSumItems[i]).doubleValue();
//        }
//        RealVector sum = new ArrayRealVector(sumArray);
//        RealVector sSum = new ArrayRealVector(sSumArray);
//        TweetCluster geoTweetCluster = new TweetCluster(id, sum, sSum, ts1, ts2, num);
//        //Map<Integer, Double> centroid = geoTweetCluster.getCentroid();
//        if (type == 1) {
//            geoTweetCluster.idSet = new HashSet<Integer>();
//            String[] idArray = items[7].split(" ");
//            for (String idString : idArray) {
//                int mergedID = new Integer(idString);
//                geoTweetCluster.idSet.add(mergedID);
//            }
//        }
//        return geoTweetCluster;
//    }

}

