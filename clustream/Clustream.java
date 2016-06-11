package clustream;

import geo.GeoTweet;
import org.apache.commons.math3.linear.RealVector;
import utils.Utils;

import java.io.*;
import java.util.*;

public class Clustream {
    
    // to test whether a tweet should form a new cluster
    double mbsFactor = 0.8;
    // max number of clusters that trigger merge operation, and the merge coefficient
    int maxNumCluster= 100;
    double mc = 0.7;
    // number of tweets, used to periodically delete outdated clusters.
    int numTweetPeriod = 2000;
    // number of tweets that have been processed so far.
    int tweetCnt = 0;
    // to test whether a cluster is outdated.
    long outdatedThreshold = 3600 * 24;
	double quantile = Utils.getQuantile(0.95);
    // current timestamp
	long currentTimestamp = -1;
    // cluster id that is to be assigned
    int toAssignClusterId = 0;
    // the clusters
	Map<Integer, MicroCluster> clusters = new HashMap<Integer, MicroCluster>();
    // pyramid time frame that stores the snapshots
	Pyramid ptf = new Pyramid();
    // stats
    int numOfProcessedTweets = 0; // total number of tweets that have been processed.
	int noHitCount = 0;
	int outdatedCount = 0; // number of outdated clusters
	int mergeNumber = 0; // number of merge.
    double elapsedTime = 0; // elapsed time


    /***************************  Construction ****************************/
	public Clustream(int maxNumCluster, int numTweetPeriod, long outdatedThreshold) {
        this.maxNumCluster = maxNumCluster;
        this.numTweetPeriod = numTweetPeriod;
        this.outdatedThreshold = outdatedThreshold;
	}

    /***************************  Initialization ****************************/
    public void init(List<GeoTweet> initialTweets, int initNumCluster) throws IOException {
        // kmeans clustering
        List<Integer> [] kmeansResults = kmeansClustering(initialTweets, initNumCluster);
        // generate initial geo tweet clusters
        genInitialGeoTweetClusters(initialTweets, kmeansResults);
    }

    private List<Integer> [] kmeansClustering(List<GeoTweet> initialTweets, int initNumCluster) {
        int kmeansMaxIter = 50;
        KMeans kmeans = new KMeans(kmeansMaxIter);
        // convert the tweets to real vectors to perform kmeans clustering.
        List<RealVector> data = new ArrayList<RealVector>();
        for (GeoTweet tweet : initialTweets) {
            data.add(tweet.getLocation().toRealVector());
        }
        // weights are null
        return kmeans.cluster(data, null, initNumCluster);
    }

    private void genInitialGeoTweetClusters(List<GeoTweet> tweets, List<Integer> [] clusteringResults) {
        for (List<Integer> memberTweetIndices : clusteringResults) {
            List<GeoTweet> memberTweets = new ArrayList<GeoTweet>();
            for (int index : memberTweetIndices) {
                memberTweets.add(tweets.get(index));
            }
            MicroCluster cluster = new MicroCluster(memberTweets, toAssignClusterId);
            clusters.put(toAssignClusterId, cluster);
            toAssignClusterId ++;
        }
    }

    /***************************  Clustering ****************************/
    // Note: the tweets should come in the ascending order of timestamp.
    public void update(GeoTweet tweet) throws IOException {
        long start = System.currentTimeMillis();
        currentTimestamp = tweet.getTimestamp();
        tweetCnt ++;
        // 2.1 try to absorb this tweet to existing clusters
        int chosenID = findToMergeCluster(tweet.getLocation().toRealVector());
        if (chosenID < 0)	// 2.1.1 no close cluster exists, create a new cluster
            createNewCluster(tweet);
        else	// 2.1.2 Data fits, put into cluster and be happy
            clusters.get(chosenID).absorb(tweet);
        // 2.2 periodically check outdated clusters
        if (tweetCnt % numTweetPeriod == 0) {
            removeOutdated();
        }
        // 2.3 if cluster number reach a limit, merge clusters
        if (clusters.size() > maxNumCluster) {
            mergeCluster();
            mergeNumber ++;
        }
        // 2.4 update the pyramid structure if necessary.
        updatePyramid();
        // 2.5 update the stats
        long end = System.currentTimeMillis();
        elapsedTime += (end - start) / 1000.0;
        numOfProcessedTweets += 1;
    }


    private int findToMergeCluster(RealVector loc) {
        MicroCluster nearestCluster = findNearestCluster(loc);
        double dist = loc.getDistance(nearestCluster.getCentroid());
        // Check whether tweet fits into closest cluster
        if (dist > computeMBS(nearestCluster)) {
            noHitCount ++;
            return -1;
        }
        else
            return nearestCluster.getId();
    }

    // Find the closest cluster
    private MicroCluster findNearestCluster(RealVector loc) {
        double minDist = Double.MAX_VALUE;
        MicroCluster nearestCluster = null;
        // get the closest cluster
        for (MicroCluster cluster : clusters.values()) {
            RealVector centroid = cluster.getCentroid();
            double dist = loc.getDistance(centroid);
            if (dist < minDist) {
                nearestCluster = cluster;
                minDist = dist;
            }
        }
        return nearestCluster;
    }

    private double computeMBS(MicroCluster cluster) {
        double boundaryDistance;
        int size = cluster.size();
        RealVector centroid = cluster.getCentroid();
        if (size > 1) {
            // when there are multiple points, calc the RMS as the boundary distance
            double squareSum = cluster.getSquareSum().getL1Norm();
            double centroidNorm = centroid.getNorm();
            boundaryDistance = Math.sqrt(squareSum / size - centroidNorm * centroidNorm);
        } else {
            // if there is only one point in the cluster, find the distance to the nearest neighbor
            boundaryDistance = Double.MAX_VALUE;
            for (MicroCluster neighbor : clusters.values()) {
                // do not count the cluster itself
                if (neighbor.getId() == cluster.getId())
                    continue;
                RealVector otherCentroid = neighbor.getCentroid();
                double dist = otherCentroid.getDistance(centroid);
                if (dist < boundaryDistance)
                    boundaryDistance = dist;
            }
        }
        return boundaryDistance * mbsFactor;
    }


    // create a new cluster for a single point.
    void createNewCluster(GeoTweet tweet) {
        List<GeoTweet> list = new ArrayList<GeoTweet>();
        list.add(tweet);
        clusters.put(toAssignClusterId, new MicroCluster(list, toAssignClusterId));
        toAssignClusterId ++;
    }


    // delete outdated clusters
    void removeOutdated() {
        List<Integer> removeIDs = new ArrayList<Integer>();
        for (MicroCluster cluster : clusters.values()) {
            // Try to forget old clusters
            double freshness = cluster.getFreshness(quantile);
            if (currentTimestamp - freshness > outdatedThreshold)
                removeIDs.add(cluster.getId());
        }
        for (Integer id : removeIDs)
            clusters.remove(id);
        outdatedCount += removeIDs.size();
    }


    // Merge existing clusters.
	void mergeCluster() {
        // the number of merge operations that need to be performed
        double toMergeCnt = clusters.size() * (1 - mc);
        // create a heap
        Queue<MicroClusterPair> pairHeap = createHeap();
        // compute pair-wise similarities among current clusters and update the heap.
        updateHeap(pairHeap);
        // mergeMap is used to record the merge history: <original cluster id, new cluster id>
		Map<Integer, Integer> mergeMap = new HashMap<Integer, Integer>();
        int mergeCnt = 0;
		while (!pairHeap.isEmpty()) {
			if (mergeCnt >= toMergeCnt)
				break;
			MicroClusterPair pair = pairHeap.poll();
			int idA = pair.clusterA.getId();
			int idB = pair.clusterB.getId();
			boolean mergedA = mergeMap.containsKey(idA);
			boolean mergedB = mergeMap.containsKey(idB);
			if (!mergedA && !mergedB) {
                // when neither A and B have been merged before, merge B into A, and delete B from the current cluster set.
				pair.clusterA.merge(pair.clusterB);
				mergeMap.put(idA, idA);
				mergeMap.put(idB, idA);
				this.clusters.remove(idB);
			}
			else if (mergedA && !mergedB) {
                // when A has been merged before and B has not, merge B into A, and delete B from the list.
				int bigClusterId = mergeMap.get(idA);
				MicroCluster bigCluster = clusters.get(bigClusterId);
				bigCluster.merge(pair.clusterB);
				mergeMap.put(idB, bigClusterId);
				this.clusters.remove(idB);
			}
			else if (!mergedA && mergedB) {
                // when B has been merged and A has not, merge A into B, and delete A from the list.
				int bigClusterId = mergeMap.get(idB);
				MicroCluster bigCluster = clusters.get(bigClusterId);
				bigCluster.merge(pair.clusterA);
				mergeMap.put(idA, bigClusterId);
                this.clusters.remove(idA);
			}
			else {
                // when A and B have both been merged, check whether they belong to the same composite cluster, if yes, then no action
                // otherwise, merge bigB to bigA.
				int bigClusterIdA = mergeMap.get(idA);
				int bigClusterIdB = mergeMap.get(idB);
				if (bigClusterIdA != bigClusterIdB) {
					MicroCluster bigClusterA = clusters.get(bigClusterIdA);
					bigClusterA.merge(clusters.get(bigClusterIdB));
					// update the member clusters of bigClusterB in merge map
					Set<Integer> needUpdate = new HashSet<Integer>();
					for (Map.Entry<Integer, Integer> entry : mergeMap.entrySet()) {
						if (entry.getValue() == bigClusterIdB)
							needUpdate.add(entry.getKey());
					}
					for (Integer updateID : needUpdate)
						mergeMap.put(updateID, bigClusterIdA);
					this.clusters.remove(bigClusterIdB);
				}
			}
            mergeCnt ++;
		}
	}

    // create a heap that has the ascending order of cluster distance, the head corresponds to the minimum distance
    private Queue<MicroClusterPair> createHeap() {
		return new PriorityQueue<MicroClusterPair>(100,
				new Comparator<MicroClusterPair>() {
					public int compare(MicroClusterPair t1, MicroClusterPair t2) {
						if (t1.getDist() < t2.getDist()) return -1;
						if (t1.getDist() > t2.getDist()) return 1;
						return 0;
					}
				});
    }

    private void updateHeap(Queue<MicroClusterPair> pairHeap) {
        List<MicroCluster> list = new ArrayList<MicroCluster>(clusters.values());
        for (int i = 0; i < list.size(); i++) {
            RealVector centroidA = list.get(i).getCentroid();
            for (int j = i+1; j < list.size(); j++) {
                RealVector centroidB = list.get(j).getCentroid();
                double dist = centroidA.getDistance(centroidB);
                pairHeap.add(new MicroClusterPair(list.get(i), list.get(j), dist));
            }
        }
    }


    /***************************  Pyramid Time Frame ****************************/
    public Pyramid getPyramid() {
        return ptf;
    }

    private void updatePyramid() {
        // check whether the current timestamp is a new time frame
        if (ptf.isNewTimeFrame(currentTimestamp)) {
            ptf.storeSnapshot(currentTimestamp, clusters);
        }
    }


    /***************************  I/O and utils ****************************/
    // get the most updated clusters
    public Set<MicroCluster> getRealTimeClusters() {
        return new HashSet<MicroCluster>(clusters.values());
    }




    public void writeClusters(BufferedWriter bwLoc, BufferedWriter bwWord) throws IOException {
        for (MicroCluster cluster : clusters.values()) {
            bwLoc.append(cluster.getCentroid().toString() + ',');
            bwWord.append(cluster.getWords().toString() + ',');
        }
        bwLoc.append("\n");
        bwWord.append("\n");
    }

	public void printStat() {
		String s = "Clustream Stats:";
		s += " current timestamp: " + currentTimestamp;
        s += "; # processed tweets=" + numOfProcessedTweets;
        s += "; elapsedTime=" + elapsedTime;
		s += "; current # cluster=" + clusters.size();
		s += "; noHitCount=" + noHitCount;
		s += "; outdatedCount=" + outdatedCount;
		s += "; mergeNumber=" + mergeNumber;
        s += "; pyramid size=" + ptf.size();
//        s += ptf.printStats();
		System.out.println(s);
	}

    public int getNumOfProcessedTweets() {
        return numOfProcessedTweets;
    }

    public double getElapsedTime() {
        return elapsedTime;
    }

    /***************************  Main method ****************************/
    public static void main(String [] args) throws Exception {
        String tweetFile = "/Users/chao/Dataset/Twitter/input/tweets-reverse.txt";
        String clusterLocFile = "/Users/chao/Dataset/Twitter/output/stream-clusters-loc.txt";
        String clusterWordFile = "/Users/chao/Dataset/Twitter/output/stream-clusters-word.txt";
        int initNumTweets = 2000;
        int initNumClusters = 100;
        int maxNumCluster = 500;
        int numTweetPeriod = 2000;
        long outdatedThreshold = 3600 * 24;
        long startTimestamp = 180000;
        long endTimestamp = 220000;

        Clustream clustream = new Clustream(maxNumCluster, numTweetPeriod, outdatedThreshold);
        BufferedReader streamReader = new BufferedReader(new FileReader(tweetFile));
        BufferedWriter bwLoc = new BufferedWriter(new FileWriter(clusterLocFile, false));
        BufferedWriter bwWord = new BufferedWriter(new FileWriter(clusterWordFile, false));

        // initialization
        List<GeoTweet> initialTweets = readInitialTweets(streamReader, initNumTweets);
        clustream.init(initialTweets, initNumClusters);
        System.out.println("Clustream initialization finished");
        clustream.printStat();

        // update
        GeoTweet tweet;
        int tweetCnt = 0;
        while ((tweet = getNext(streamReader)) != null) {
            clustream.update(tweet);
            tweetCnt ++;
            if (tweetCnt % numTweetPeriod == 0) {
                System.out.println("Tweet count:" + tweetCnt);
                clustream.printStat();
                clustream.writeClusters(bwLoc, bwWord);
            }
        }

        // get the clusters that are in the past time horizon
        Snapshot startSnapshot = clustream.getPyramid().getSnapshotJustBefore(startTimestamp);
        Snapshot endSnapshot = clustream.getPyramid().getSnapshotJustBefore(endTimestamp);
        Set<MicroCluster> clusters = endSnapshot.getDiffClusters(startSnapshot);
        System.out.println(clusters);

        streamReader.close();
        bwLoc.close();
        bwWord.close();
    }

    static List<GeoTweet> readInitialTweets(BufferedReader streamReader, int initNumTweet) throws IOException {
        List<GeoTweet> initialTweets = new ArrayList<GeoTweet>();
        for (int i=0; i<initNumTweet; i++) {
            String line = streamReader.readLine();
            GeoTweet tweet = new GeoTweet(line);
            initialTweets.add(tweet);
        }
        return initialTweets;
    }

    static GeoTweet getNext(BufferedReader streamReader) throws IOException {
        String line;
        if((line = streamReader.readLine()) != null) {
            return new GeoTweet(line);
        } else {
            return null;
        }
    }

}


