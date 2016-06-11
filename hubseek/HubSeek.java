package hubseek;

import geo.GeoTweet;
import graph.Graph;

import java.util.*;

/**
 * This class performs the batch hubseek algorithm, and also supports
 * online deletion, insertion of new tweets to update the clustering results.
 * Created by chao on 6/20/15.
 */
public class HubSeek {

    double bandwidth;
    double epsilon;
    Graph entityGraph;
    Map<Long, GeoTweet> points; // <tweet Id, tweet>
    Map<Long, Set<Long>> neighborhood;
    Map<Long, Double> scores;
    Map<Long, Long> localHubs;
    Map<Long, Long> globalHubs;

    // statistics
    int numBatchTweets; // number of tweets in the current window.
    int numDeletedTweet; // number of tweets that have been deleted.
    int numInsertedTweet; // number of tweets that have been inserted.
    double timeBatchClustering; // elapsed time for clustering the tweets in a batch mode.
    double timeDeletion; // elapsed time for deletion
    double timeInsertion; // elapsed time for insertion.

    public HubSeek(double bandwidth, double epsilon, Graph entityGraph) {
        this.bandwidth = bandwidth;
        this.epsilon = epsilon;
        this.entityGraph = entityGraph;
    }

    /**
     * cluster the given points in a batch mode.
     */
    public void cluster(List<GeoTweet> inputData) {
        long start = System.currentTimeMillis();

        init(inputData);
        calcNeighborhood();
        calcScores();
        findLocalHubs();

        // write the stats
        long end = System.currentTimeMillis();
        numBatchTweets = inputData.size();
        timeBatchClustering = (end - start) / 1000.0;
    }


    private void init(List<GeoTweet> inputData) {
        // init input data points
        points = new HashMap<Long, GeoTweet>();
        for (GeoTweet t : inputData) {
            points.put(t.getTweetId(), t);
        }
        // init neighbhorhood
        neighborhood = new HashMap<Long, Set<Long>>();
        // init the rest arrays
        scores = new HashMap<Long, Double>();
        localHubs = new HashMap<Long, Long>();
        globalHubs = new HashMap<Long, Long>();
    }


    private void calcNeighborhood() {
        for (Long tid : points.keySet()) {
            // find the in-neighbors among points for e
            Set<Long> inNeighbors = findInNeighbors(tid);
            neighborhood.put(tid, inNeighbors);
//            System.out.println("Neighborhood size" + inNeighbors.size());
        }
    }

    private void calcScores() {
        for (Long tid : points.keySet()) {
            // calc score for e using the current points
            double score = calcScore(tid);
            scores.put(tid, score);
        }
    }

    private void findLocalHubs() {
        for (Long tid : points.keySet()) {
            Long localHub = findLocalHubForOnePoint(tid);
            localHubs.put(tid, localHub);
        }
    }

    /**
     * delete some points, update: points, neighborhood, and scores
     */

    public void delete(List<GeoTweet> oldPoints) {
        long start = System.currentTimeMillis();
        initForDelete(oldPoints);
        // get the out neighborhood for the points that need to be deleted
        Map<Long, Set<Long>> outNeighborhood = findOutNeighborhood(oldPoints);
        // update the in-neighborhood for the existing points
        updateNeighborhoodForDelete(outNeighborhood);
        // update the scores for existing points, note that the out neighborhood is defined on updated points
        updateScoresForDelete(oldPoints, outNeighborhood);
        // update the local hubs.
        updateLocalHubs();
        // write the stats
        long end = System.currentTimeMillis();
        numDeletedTweet = oldPoints.size();
        timeDeletion = (end - start) / 1000.0;
    }


    // init the points and other data fields
    // parameter: the old points that need to be deleted.
    private void initForDelete(List<GeoTweet> oldPoints) {
        for (GeoTweet e : oldPoints) {
            Long tid = e.getTweetId();
            points.remove(tid);
            neighborhood.remove(tid);
            scores.remove(tid);
            localHubs.remove(tid);
            globalHubs.remove(tid);
        }
    }


    // update the neighborhood. Input: the out neighborhood for the old points.
    private void updateNeighborhoodForDelete(Map<Long, Set<Long>> outNeighborhood) {
        for (Long tid : outNeighborhood.keySet()) {
            Set<Long> outNeighbors = outNeighborhood.get(tid);
            for (Long neighbor : outNeighbors)
                // remove e from the in-neighborhood
                neighborhood.get(neighbor).remove(tid);
        }
    }


    // return the remaining tweets whose scores have been updated
    private void updateScoresForDelete(List<GeoTweet> oldPoints, Map<Long, Set<Long>> outNeighborhood) {
        for (GeoTweet e : oldPoints) {
            // update the scores for the out neighbors of e.
            Set<Long> outNeighborIds = outNeighborhood.get(e.getTweetId());
            for (Long neighborId : outNeighborIds) {
                GeoTweet neighbor = points.get(neighborId);
                double deltaScore = neighbor.calcScoreFrom(e, bandwidth, entityGraph);
                double updatedScore = scores.get(neighborId) - deltaScore;
                scores.put(neighborId, updatedScore);
            }
        }
    }

    // update the local hubs for the remaining points
    private void updateLocalHubs() {
        for (Long e : points.keySet()) {
            Long localHub = findLocalHubForOnePoint(e);
            localHubs.put(e, localHub);
        }
    }


    /**
     * insert new data
     */
    public void insert(List<GeoTweet> insertData) {
        long start = System.currentTimeMillis();
        // update the points
        initForInsert(insertData);
        // get the out neighborhood for the points that need to be inserted
        Map<Long, Set<Long>> outNeighborhood = findOutNeighborhood(insertData);
        // update the in-neighborhood for the existing points
        updateNeighborhoodInsert(outNeighborhood);
        // update the scores for existing points
        updateScoresInsert(insertData, outNeighborhood);
        // update the local hubs.
        updateLocalHubs();
        // write the stats
        long end = System.currentTimeMillis();
        numInsertedTweet = insertData.size();
        timeInsertion = (end - start) / 1000.0;

    }


    // update the fields for insertion.
    private void initForInsert(List<GeoTweet> insertData) {
        for (GeoTweet e : insertData) {
            long tid = e.getTweetId();
            points.put(tid, e);
            neighborhood.put(tid, new HashSet<Long>());
            scores.put(tid, 0d);
            localHubs.put(tid, -1L);
            globalHubs.put(tid, -1L);
        }
    }


    private void updateNeighborhoodInsert(Map<Long, Set<Long>> outNeighborhood) {
        for (Long e : outNeighborhood.keySet()) {
            // add e into the in-neighbor set for e's out-neighbors
            Set<Long> outNeighbors = outNeighborhood.get(e);
            for (Long neighbor : outNeighbors) {
                neighborhood.get(neighbor).add(e);
            }
            // create the in-neighbor set for e itself
            Set<Long> inNeighbors = findInNeighbors(e);
            neighborhood.put(e, inNeighbors);
        }
    }


    // return the set of geo tweets whose scores have been updated
    private void updateScoresInsert(List<GeoTweet> insertData, Map<Long, Set<Long>> outNeighborhood) {
        // Update the scores for the old points
        for (GeoTweet e: insertData) {
            // update the scores for the out neighbors of e.
            Set<Long> outNeighborIds = outNeighborhood.get(e.getTweetId());
            for (Long neighborId : outNeighborIds) {
                GeoTweet neighbor = points.get(neighborId);
                double deltaScore = neighbor.calcScoreFrom(e, bandwidth, entityGraph);
                // if the neighbor is the entity itself and the entity is new, then old score is 0.
                double updatedScore = scores.get(neighborId) + deltaScore;
                scores.put(neighborId, updatedScore);
            }
        }
        // calc the scores for the new points
        for (GeoTweet e: insertData) {
            // note that: the new points in the neighborhood is recomputed
            double score = calcScore(e.getTweetId());
            scores.put(e.getTweetId(), score);
        }
    }


    /**
     * generate the clusters.
     */
    public List<TweetCluster> genClusters(double supportThreshold) {
        findGlobalHubs();
        Map<Long, TweetCluster> clusters = new HashMap<Long, TweetCluster>(); // key: mode, value: cluster
        for (GeoTweet e : points.values()) {
            Long globalHub = globalHubs.get(e.getTweetId());
            if (clusters.containsKey(globalHub)) {
                TweetCluster gec = clusters.get(globalHub);
                gec.add(e, scores.get(e.getTweetId()));
            } else {
                // create a new cluster centered at the mode
                GeoTweet hub = points.get(globalHub);
                TweetCluster gec = new TweetCluster(hub);
                gec.add(e, scores.get(e.getTweetId()));
                clusters.put(globalHub, gec);
            }
        }
        // prune the clusters by size.
        List<TweetCluster> results = new ArrayList<TweetCluster>();
        for(Map.Entry e : clusters.entrySet()) {
            TweetCluster c = (TweetCluster) e.getValue();
            if (c.size() >= supportThreshold)
                results.add(c);
        }
        return results;
    }


    private void findGlobalHubs() {
        int cnt;
        for (Long e : points.keySet()) {
            cnt = 0;
            Long currentPoint = e;
            while(true) {
                Long localHub = localHubs.get(currentPoint);
                if(currentPoint.equals(localHub))
                    break;
                cnt += 1;
                if (cnt >= 1000) {
                    System.out.println("current:" + currentPoint + "local hub:" + localHub);
                    System.out.println(scores.get(currentPoint));
                    System.out.println(neighborhood.get(currentPoint));
                    System.out.println(neighborhood.get(localHub));
                }
                if (cnt >= 1010) {
                    System.out.println("Finding global hubs error.");
                    System.exit(1);
                }

                currentPoint = localHub;
            }
            globalHubs.put(e, currentPoint);
        }
    }

    /**
     * utils functions
     */
    // find the in-neighbors for one geo-tweet
    private Set<Long> findInNeighbors(Long tid) {
        Set<Long> neighbors = new HashSet<Long>();
        GeoTweet e = points.get(tid); // query tweet
        for (GeoTweet other : points.values()) {
            double geoDist = e.calcGeoDist(other);
            double graphProximity = e.calcGraphDistFrom(entityGraph, other);
            if (geoDist <= bandwidth && graphProximity >= epsilon)
                neighbors.add(other.getTweetId());
        }
//        neighbors.add(tid); // add the tweet itself into the set
        if (neighbors.size() == 0) {
            System.out.println("no neighbor:" + e);
        }
        return neighbors;
    }

    // find the out-neighbors for one geo-entity
    private Set<Long> findOutNeighbors(GeoTweet e) {
        Set<Long> neighbors = new HashSet<Long>();
        for (GeoTweet other : points.values()) {
            double geoDist = e.calcGeoDist(other);
            double graphProximity = other.calcGraphDistFrom(entityGraph, e);
            if (geoDist <= bandwidth && graphProximity >= epsilon)
                neighbors.add(other.getTweetId());
        }
        return neighbors;
    }


    private Map<Long, Set<Long>> findOutNeighborhood(List<GeoTweet> points) {
        Map<Long, Set<Long>> outNeighborhood = new HashMap<Long, Set<Long>>();
        for (GeoTweet e : points) {
            Set<Long> outNeighbors = findOutNeighbors(e);
            outNeighborhood.put(e.getTweetId(), outNeighbors);
        }
        return outNeighborhood;
    }


    private double calcScore(Long tid) {
        double score = 0;
        GeoTweet e = points.get(tid);
        Set<Long> neighborIds = neighborhood.get(tid);
        for (Long nid : neighborIds) {
            GeoTweet neighbor = points.get(nid);
            score += e.calcScoreFrom(neighbor, bandwidth, entityGraph);
        }
        return score;
    }


    private Long findLocalHubForOnePoint(Long tid) {
        Long localHub = null;
        double maxScore = -1.0;
        Set<Long> neighbors = neighborhood.get(tid);
        // if the tweet does not have any neighbor at all, return the tweet itself.
        if (neighbors.size() == 0)
            return tid;
        for (Long neighbor : neighbors) {
            double score = scores.get(neighbor);
            if(score > maxScore) {
                maxScore = score;
                localHub = neighbor;
            } else if (score == maxScore && localHub != null && neighbor > localHub) {
                localHub = neighbor;
            }
        }
        return localHub;
    }


    public void printStat() {
        String s = "HubSeek Stats:";
        s += " numBatchTweets=" + numBatchTweets;
        s += "; numDeletedTweets=" + numDeletedTweet;
        s += "; numInsertedTweets=" + numInsertedTweet;
        s += "; timeBatchClustering=" + timeBatchClustering;
        s += "; timeDeletion=" + timeDeletion;
        s += "; timeInsertion=" + timeInsertion;
        System.out.println(s);
    }

    public int getNumBatchTweets() {
        return numBatchTweets;
    }

    public int getNumDeletedTweet() {
        return numDeletedTweet;
    }

    public int getNumInsertedTweet() {
        return numInsertedTweet;
    }

    public double getTimeBatchClustering() {
        return timeBatchClustering;
    }

    public double getTimeDeletion() {
        return timeDeletion;
    }

    public double getTimeInsertion() {
        return timeInsertion;
    }

}



//    // update the local hubs for the remaining points
//    private void updatelocalhubs(set<long> scorechangedpoints) {
//        // find the points whose local hubs need to be updated.
//        set<long> toupdatepoints = findtoupdatepoints(scorechangedpoints);
////        system.out.println("# to update points:" + toupdatepoints.size());
//        for (long e : toupdatepoints) {
//            long localhub = findlocalhubforonepoint(e);
//            localhubs.put(e, localhub);
//        }
//    }



//    // for point p whose score has been changed, we need to update all the points who have p as an in-neighbor
//    private Set<Long> findToUpdatePoints(Set<Long> scoreChangedPoints) {
//        Set<Long> results = new HashSet<Long>();
//        for (Long tid : scoreChangedPoints) {
//            Set<Long> outNeighbors;
//            // reuse the neighborhood info.
////            if (outNeighborhood.containsKey(tid))
////                outNeighbors = outNeighborhood.get(tid);
////            else
//            outNeighbors = findOutNeighbors(points.get(tid));
//            results.addAll(outNeighbors);
//        }
//        return results;
//    }
