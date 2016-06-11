package hubseek;

import com.mongodb.BasicDBObject;
import geo.GeoTweet;

import java.util.*;

/**
 * Created by chao on 6/20/15.
 */
public class TweetCluster {

    int clusterId;
    List<GeoTweet> members = new ArrayList<GeoTweet>();
    List<Double> authority = new ArrayList<Double>();  // the authority scores for the members
    GeoTweet center;
    Map<Integer, Double> distribution = new HashMap<Integer, Double>(); // key: entity id; value: probability
    Map<Integer, Double> tfIdf = new HashMap<Integer, Double>(); // key: entity id; value: tf-idf
    // the score of this cluster
    double score = 0;

    public TweetCluster(GeoTweet center) {
        this.center = center;
    }

    public void add(GeoTweet e, double score) {
        members.add(e);
        authority.add(score);
    }

    public double getScore() {
        return score;
    }

    public int getId() {
        return clusterId;
    }

    public GeoTweet getCenter() {
        return center;
    }

    public int size() {
        return members.size();
    }

    public Set<Integer> getEntityIds() {
        return distribution.keySet();
    }

    public double getEntityProb(int entityId) {
        return distribution.containsKey(entityId) ? distribution.get(entityId) : 0;
    }

    // generate the probability distribution of entities.
    public void genProbDistribution() {
        Map<Integer, Double> entityOccurrence = getEntityOccurrences();
        double totalWeight = 0;
        for (Double weight : entityOccurrence.values()) {
            totalWeight += weight;
        }
        for (Integer entityId : entityOccurrence.keySet()) {
            double probability = entityOccurrence.get(entityId) / totalWeight;
            distribution.put(entityId, probability);
        }
    }


    // get a map that store the number of occurrences for different entities
    public Map<Integer, Double> getEntityOccurrences() {
        Map<Integer, Double> occurrences = new HashMap<Integer, Double>();
        for (GeoTweet e : members) {
            List<Integer> entityIds = e.getEntityIds();
            for (Integer entityId : entityIds) {
                double originalCnt = occurrences.containsKey(entityId) ? occurrences.get(entityId) : 0;
                occurrences.put(entityId, originalCnt + 1);
            }
        }
        return occurrences;
    }


    public void setScore(double score) {
        this.score = score;
    }

    // val is the tf-idf value for the entity
    public void setTfIdf(int entityId, double val) {
        tfIdf.put(entityId, val);
    }


    @Override
    public String toString() {
        String s = "# Cluster Score:" + score + "\n";
        s += "Num of Tweets:" + this.members.size() + "\n";
        s += "Center Tweet ID:" + this.center.getTweetId() + "\n";
        for (GeoTweet e : members) {
            s += e.toString() + "\n";
        }
        return s;
    }


    public BasicDBObject toBSon() {
        // members
        List<Long> tweetIds = new ArrayList<Long>();
        for (int i=0; i < members.size(); i++) {
            GeoTweet e = members.get(i);
            tweetIds.add(e.getTweetId());
        }
        BasicDBObject entities = new BasicDBObject();
        for (Integer entityId : tfIdf.keySet()) {
            double tfIdfVal = tfIdf.get(entityId);
            entities.append(entityId.toString(), tfIdfVal);
        }
        return new BasicDBObject()
                .append("score", score)
                .append("center", center.getTweetId())
                .append("size", tweetIds.size())
                .append("members", tweetIds)
                .append("authority", authority)
                .append("entityTfIdf", entities);
    }

}
