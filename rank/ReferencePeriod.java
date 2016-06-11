package rank;

import clustream.MicroCluster;
import clustream.Snapshot;
import geo.Location;

import java.util.*;

/**
 * Created by chao on 6/28/15.
 * This class is the background knowledge for a specific time period.
 */
public class ReferencePeriod {

    double smoothingCnt = 0.001;
    Snapshot startSnapshot;
    Snapshot endSnapshot;
    long startTimestamp;
    long endTimestamp;
    List<MicroCluster> clusters;
    List<Double> weights;


    public ReferencePeriod(Snapshot startSnapshot, Snapshot endSnapshot) {
        this.startSnapshot = startSnapshot;
        this.endSnapshot = endSnapshot;
        this.startTimestamp = startSnapshot.getTimestamp();
        this.endTimestamp = endSnapshot.getTimestamp();
        this.clusters = new ArrayList<MicroCluster>(endSnapshot.getDiffClusters(startSnapshot));
    }

    // get the expected number of occurrences for a given set of keywords at a location during a time span.
    public Map<Integer, Double> getExpectedOccurrences(Set<Integer> wordIds, Location loc, double bandwidth, long timeSpan) {
        calcWeights(loc, bandwidth);
        double scalingFactor = getScalingFactor(timeSpan);
        Map<Integer, Double> expectedOccurrences = new HashMap<Integer, Double>();
        for (Integer wordId : wordIds) {
            double interpolation = getEstimatedNumber(wordId);
            expectedOccurrences.put(wordId, interpolation * scalingFactor);
        }
        return expectedOccurrences;
    }

//    // calculate the weights of the cluster centers to the query location
//    private void calcWeights(Location loc) {
//        weights = new ArrayList<Double>();
//        double totalWeight = 0;
//        for (TweetCluster cluster : clusters) {
//            double dist = smoothingDist + cluster.getCentroid().getDistance(loc.toRealVector());
//            double weight = 1.0 / dist;
//            weights.add(weight);
//            totalWeight += weight;
//        }
//        // normalize the weights
//        for (int i=0; i<weights.size(); i++) {
//            double normalizedWeight = weights.get(i) / totalWeight;
//            weights.set(i, normalizedWeight);
//        }
//    }

    // calculate the weights of the cluster centers to the query location
    private void calcWeights(Location loc, double bandwidth) {
        weights = new ArrayList<Double>();
        for (MicroCluster cluster : clusters) {
            double dist = cluster.getCentroid().getDistance(loc.toRealVector());
            double weight = 0;
            if (dist < bandwidth) {
                weight = 1.0 - (dist / bandwidth) * (dist / bandwidth);
            }
            weights.add(weight);
        }
    }

    private double getScalingFactor(long timeSpan) {
        return (double) timeSpan / (double) (endTimestamp - startTimestamp);
    }

    // use kernel to get the estimated occurrence for the given word
    private double getEstimatedNumber(int wordId) {
        double result = 0;
        for (int i = 0; i<clusters.size(); i++) {
            MicroCluster cluster = clusters.get(i);
            double weight = weights.get(i);
            Map<Integer, Integer> wordsInCluster = cluster.getWords();
            double wordCntInCluster = wordsInCluster.containsKey(wordId) ? wordsInCluster.get(wordId) : smoothingCnt;
            result += weight * wordCntInCluster;
        }
        return result;
    }

}
