package demo;

import geo.GeoTweet;
import geo.TweetDatabase;
import graph.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by chao on 4/29/15.
 */
public class Database {

    BufferedReader streamReader;
    TweetDatabase initialTweets; // the set of initial tweets, used to initialize the pyramid.
    Graph graph; // The entity graph.

    public Graph getEntityGraph() {
        return graph;
    }

    public TweetDatabase getInitialTweets() {
        return initialTweets;
    }

    public void loadEntityGraph(String nodeFile, String edgeFile) throws Exception {
        graph = new Graph();
        graph.loadNodes(nodeFile);
        graph.loadEdges(edgeFile, false);
    }

    // load the initial tweets and get the stream reader ready.
    public void loadInitialTweets(String tweetFile, int numInitTweets) throws Exception {
        initialTweets = new TweetDatabase();
        streamReader = new BufferedReader(new FileReader(tweetFile));
        for (int i=0; i<numInitTweets; i++) {
            String line = streamReader.readLine();
            GeoTweet tweet = new GeoTweet(line);
            initialTweets.add(tweet);
        }
        System.out.println("Finished loading initial tweets. Count:" + initialTweets.size());
    }

    public GeoTweet nextTweet() throws IOException {
        GeoTweet tweet = null;
        while (tweet == null || tweet.numEntity() == 0) {
            String line = streamReader.readLine();
            if (line == null)
                return null;
            tweet = new GeoTweet(line);
        }
        return tweet;
    }

}
