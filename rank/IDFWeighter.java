package rank;

import geo.GeoTweet;
import hubseek.TweetCluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.Math;

/**
 * Created by chao on 6/29/15.
 */
public class IDFWeighter {

    // the total number of documents in the collection.
    int N;
    Map<Integer, Double> idfs;
    Map<Integer, Double> tfs;
    // normalized tf-idf weights
    Map<Integer, Double> weights;

    public IDFWeighter(List<GeoTweet> tweets) {
        buildIDF(tweets);
    }

    // build the idfs with all the tweets in the current time window.
    public void buildIDF(List<GeoTweet> tweets) {
        N = tweets.size();
        idfs = new HashMap<Integer, Double>();
        for (GeoTweet t : tweets) {
            List<Integer> entityIds = t.getEntityIds();
            for (Integer entityId : entityIds) {
                double originalCnt = idfs.containsKey(entityId) ? idfs.get(entityId) : 0;
                idfs.put(entityId, originalCnt + 1.0);
            }
        }
        for (Integer entityId : idfs.keySet()) {
            double n = idfs.get(entityId);
            double idf = Math.log((N - n + 0.5) / (n + 0.5));
            idfs.put(entityId, idf);
        }
    }

    // build the tfs with the tweets in the current cluster.
    public void buildTFIDF(TweetCluster cluster) {
        weights = new HashMap<Integer, Double>(); // key: entity Id, value: tf-idf
        // calc tf-idf
        tfs = cluster.getEntityOccurrences(); // this is tf
        double totalweight = 0;
        for (Integer entityId : tfs.keySet()) {
            double tf = Math.log(1.0 + tfs.get(entityId));
            double tfIdf = tf * getIDF(entityId);
            weights.put(entityId, tfIdf);
            totalweight += tfIdf;
        }
        // normalization
        for (Integer entityId : tfs.keySet()) {
            double tfIdf = weights.get(entityId) / totalweight;
            weights.put(entityId, tfIdf);
            cluster.setTfIdf(entityId, tfIdf);
        }
    }


    public double getIDF(int id) {
        return idfs.get(id);
    }

    public Map<Integer, Double> getWeights() {
        return weights;
    }

}
