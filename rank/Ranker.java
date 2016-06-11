package rank;

import clustream.Clustream;
import clustream.Snapshot;
import hubseek.TweetCluster;
import utils.ScoreCell;
import utils.Utils;

import java.util.*;

/**
 * Usage: rank the online clusters
 * Created by chao on 6/28/15.
 */
public class Ranker {

    Clustream clustream;
    List<ReferencePeriod> referencePeriods;
    IDFWeighter weighter;
    double eta; // 0 < eta < 1, a balancing factor between temporal burstiness and spatial uniqueness

    // stats
    int numClusters;
    int numReferencePeriods;
    double timeFetching; // elapsed time for fetching reference periods from clustream.
    double timeRanking; // elapsed time for ranking.

    public Ranker(Clustream clustream, IDFWeighter weighter, double eta) {
        this.clustream = clustream;
        this.weighter = weighter;
        this.eta = eta;
    }


    public List<ScoreCell> rank(List<TweetCluster> clusters, double bandwidth, long startTimestamp, long endTimestamp, long refTimespan) {
        long start = System.currentTimeMillis();
        // init the reference periods
        initReferencePeriods(startTimestamp, refTimespan);
        // init the probability distribution for each cluster
        for (TweetCluster gec : clusters) {
            gec.genProbDistribution();
        }
        long end = System.currentTimeMillis();
        timeFetching = (end - start) / 1000.0;
        numClusters = clusters.size();
        numReferencePeriods = referencePeriods.size();

        start = System.currentTimeMillis();
        List<ScoreCell> scorecells = new ArrayList<ScoreCell>();
        for (int clusterIndex = 0; clusterIndex < clusters.size(); clusterIndex++) {
            TweetCluster cluster = clusters.get(clusterIndex);
            double score = calcScore(cluster, clusters, bandwidth, startTimestamp, endTimestamp);
            cluster.setScore(score);
            ScoreCell sc = new ScoreCell(clusterIndex, score);
            scorecells.add(sc);
        }
//        System.out.println(scorecells);
        sortScoreCells(scorecells);
        end = System.currentTimeMillis();
        timeRanking = (end - start) / 1000.0;
        return scorecells;
    }

    // fetech from clustream the list of reference periods and the clusters falling inside each reference period
    private void initReferencePeriods(long startTimestamp, long refTimespan) {
        referencePeriods = new ArrayList<ReferencePeriod>();
        // get the snapshots falling in the reference window, ordered from old to new.
        List<Snapshot> snapshots = clustream.getPyramid().getSnapshotsBetween(startTimestamp - refTimespan, startTimestamp);
//        for (int i = 2; snapshots.size() <= 1; i++) {
//            System.out.println("i: " + i);
//            System.out.println("start timestamp: " + startTimestamp);
//            snapshots = clustream.getPyramid().getSnapshotsBetween(startTimestamp - refTimespan * i, startTimestamp);
//        }
        for (int i=0; i<snapshots.size()-1; i++) {
            Snapshot startSnapshot = snapshots.get(i);
            Snapshot endSnapshot = snapshots.get(i+1);
            ReferencePeriod rp = new ReferencePeriod(startSnapshot, endSnapshot);
            referencePeriods.add(rp);
        }
    }


    // get the weighted zscore as the final score for the cluster
    private double calcScore(TweetCluster cluster, List<TweetCluster> allCandidates,
                             double bandwidth, long startTimestamp, long endTimestamp) {
        Map<Integer, Double> temporalZScores = genTemporalZScores(cluster, bandwidth, startTimestamp, endTimestamp); // key: entity ID, value: probability
        Map<Integer, Double> spatialZScores = genSpatialZScores(cluster, allCandidates); // key: entity ID, value: probability
//        System.out.println("z-scores: " + zscores);
        weighter.buildTFIDF(cluster);
        Map<Integer, Double> weights = weighter.getWeights();
        double score = 0;
        for (Integer entityId : temporalZScores.keySet()) {
            double temporalZscore = temporalZScores.get(entityId);
            double spatialZscore = spatialZScores.get(entityId);
            double weight = weights.get(entityId);
            score += weight * (eta * temporalZscore + (1 - eta) * spatialZscore);
        }
        return score;
    }

    /*****************************************temporal z-score ********************************/
    // get the z-score vector for the entities in the current cluster, key: entity id, value: z-score
    private Map<Integer, Double> genTemporalZScores(TweetCluster cluster, double bandwidth, long startTimestamp, long endTimestamp) {
        // the count of occurrences in the online cluster. Key: entity Id, value: occurrence.
        Map<Integer, Double> onlineOccurrences = cluster.getEntityOccurrences();
        List<Map<Integer, Double>> referenceOccurrencesList = getReferenceOccurrencesList(cluster, bandwidth, startTimestamp, endTimestamp);
        return calcTemporalZScore(onlineOccurrences, referenceOccurrencesList);
    }

    // get the list of reference entity occurrences in the reference time window.
    private List<Map<Integer, Double>> getReferenceOccurrencesList(TweetCluster cluster, double bandwidth,
                                                                   long startTimestamp, long endTimestamp) {
        List<Map<Integer, Double>> referenceOccurrencesList = new ArrayList<Map<Integer, Double>>();
        // the set of entity ids in the online cluster
        Set<Integer> entityIds = cluster.getEntityIds();
        // get the snapshots falling in the reference window, ordered from old to new.
        for (ReferencePeriod rp : referencePeriods) {
            Map<Integer, Double> referenceOccurrences = rp.getExpectedOccurrences(entityIds,
                    cluster.getCenter().getLocation(), bandwidth, endTimestamp - startTimestamp);
            referenceOccurrencesList.add(referenceOccurrences);
        }
        return referenceOccurrencesList;
    }

    private Map<Integer, Double> calcTemporalZScore(Map<Integer, Double> onlineOccurrences, List<Map<Integer, Double>> references) {
        Map<Integer, Double> zscores = new HashMap<Integer, Double>();
        for (Map.Entry e : onlineOccurrences.entrySet()) {
            int entityId = (Integer) e.getKey();
            double cnt = (Double) e.getValue();
            // extract the reference numbers
            List<Double> referenceCounts = new ArrayList<Double>();
            for (Map<Integer, Double> referenceOccurrences : references) {
                referenceCounts.add(referenceOccurrences.get(entityId));
            }
            if (referenceCounts.size() == 0) {
                System.out.println("No reference count when computing temporal z-score!");
                zscores.put(entityId, new Double(0));
            } else {
                // calc z-score for the specific entity
                double mu = Utils.mean(referenceCounts);
                double sigma = Utils.std(referenceCounts);
                double zscore = (sigma == 0 ? 0 : (cnt - mu) / sigma);
                zscores.put(entityId, zscore);
            }
        }
        return zscores;
    }

    /***************************************** spatial z-score ********************************/
    // get the spatial z-score vector for the entities in the current cluster, key: entity id, value: spatial z-score
    private Map<Integer, Double> genSpatialZScores(TweetCluster cluster, List<TweetCluster> allCandidates) {
        Map<Integer, Double> zscores = new HashMap<Integer, Double>();
        for (Integer entityId: cluster.getEntityIds()) {
            double prob = cluster.getEntityProb(entityId);
            // extract the reference probabilities
            List<Double> refProbs = new ArrayList<Double>();
            for (TweetCluster g : allCandidates) {
                refProbs.add(g.getEntityProb(entityId));
            }
            if (refProbs.size() == 0) {
                System.out.println("No reference count when computing spatial z-score!");
                zscores.put(entityId, new Double(0));
            } else {
                // calc z-score for the specific entity
                double mu = Utils.mean(refProbs);
                double sigma = Utils.std(refProbs);
                double zscore = (sigma == 0 ? 0 : (prob - mu) / sigma);
                zscores.put(entityId, zscore);
            }
        }
        return zscores;
    }


    // rank the scorecells in the descending order of the score
    private void sortScoreCells(List<ScoreCell> scoreCells) {
        Collections. sort(scoreCells, new Comparator<ScoreCell>() {
            public int compare(ScoreCell u1, ScoreCell u2) {
                if (u1.getScore() - u2.getScore() > 0)
                    return -1;
                else if (u1.getScore() - u2.getScore() == 0)
                    return 0;
                else
                    return 1;
            }
        });
    }

    public void printStats() {
        String s = "Ranker Stats:";
        s += " numClusters=" + numClusters;
        s += "; numReferencePeriods=" + numReferencePeriods;
        s += "; timeFetchingClustream=" + timeFetching;
        s += "; timeRanking=" + timeRanking;
        System.out.println(s);
    }

    public int getNumReferencePeriods() {
        return numReferencePeriods;
    }

    public double getTimeFetching() {
        return timeFetching;
    }

    public double getTimeRanking() {
        return timeRanking;
    }

}
