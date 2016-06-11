package geo;

import graph.Graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by chao on 6/20/15.
 */
public class GeoTweet {
    long tweetId;
    long userId;
    long timestamp;
    Location loc;
    List<Integer> entityIds;

    public GeoTweet(long tweetId, long userId, long timestamp, Location loc, List<Integer> entityIds) {
        this.tweetId = tweetId;
        this.userId = userId;
        this.timestamp = timestamp;
        this.loc = loc;
        this.entityIds = entityIds;
    }

    public GeoTweet(String tweetString) {
        Scanner sr = new Scanner(tweetString);
        sr.useDelimiter("\\t");
        tweetId = sr.nextLong();
        userId = sr.nextLong();
        timestamp = sr.nextLong();
        double lng = sr.nextDouble();
        double lat = sr.nextDouble();
        loc = new Location(lng, lat);
        entityIds = new ArrayList<Integer>();
        while(sr.hasNextInt()) {
            entityIds.add(sr.nextInt());
        }
    }

    public long getTweetId() {
        return tweetId;
    }

    public int numEntity() {
        return entityIds.size();
    }


    public long getUserId() {
        return userId;
    }

    // calc the weighted spatio-semantic score received from another geo-tweet
    public double calcScoreFrom(GeoTweet e, double bandwidth, Graph entityGraph) {
        double geoScore = calcKernelScore(e, bandwidth);
        double semanticScore = e.calcGraphDistFrom(entityGraph, e);
        return geoScore * semanticScore;
    }

    // calc the geographical kernel score between two entities
    public double calcKernelScore(GeoTweet other, double bandwidth) {
        double dist = this.calcGeoDist(other);
        double kernelScore = 1.0 - (dist / bandwidth)*(dist / bandwidth); // kernel
        return kernelScore;
    }

    // calc the Euclidean distance between two geo tweets.
    public double calcGeoDist(GeoTweet other) {
        return loc.calcEuclideanDist(other.getLocation());
    }

    // calc the semantic distance between from the other tweet to this tweet
    public double calcGraphDistFrom(Graph graph, GeoTweet other) {
        double proximity = 0;
        for (int entityId : this.getEntityIds()) {
            for (int otherEntityId : other.getEntityIds()) {
                proximity += graph.getRWR(otherEntityId, entityId);
            }
        }
        int size = this.getEntityIds().size() * other.getEntityIds().size();
        return proximity / size;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Location getLocation() {
        return loc;
    }

    public List<Integer> getEntityIds() {
        return entityIds;
    }

    @Override
    public String toString() {
        return loc.toString() + "," + entityIds;
    }

}
