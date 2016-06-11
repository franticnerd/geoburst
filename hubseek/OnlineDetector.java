package hubseek;

import clustream.Clustream;
import geo.TweetDatabase;
import graph.Graph;
import rank.IDFWeighter;
import rank.Ranker;

import java.util.List;

/**
 * Online event detection.
 * Created by chao on 7/7/15.
 */
public class OnlineDetector extends BatchDetector {

    public OnlineDetector(Clustream clustream, Graph graph) {
        super(clustream, graph);
    }

    public void update(TweetDatabase deleteTweets, TweetDatabase insertTweets,
                       double bandwidth, double minSup, long refTimeSpan, double eta) {
        this.updateMode = true;
        List<TweetCluster> clusters = updateClusters(deleteTweets, insertTweets, minSup);
        updateTweetDatabase(deleteTweets, insertTweets);
        updateRanker(eta);
        // rank the clusters as events
        events = rank(clusters, bandwidth, refTimeSpan);
        // set the stats
        setStats();
    }

    private List<TweetCluster> updateClusters(TweetDatabase deleteTd, TweetDatabase insertTd, double minSup) {
        hubSeek.delete(deleteTd.getTweets());
        hubSeek.insert(insertTd.getTweets());
        return hubSeek.genClusters(minSup);
    }

    private void updateTweetDatabase(TweetDatabase deleteTd, TweetDatabase insertTd) {
        td.deleteFromHead(deleteTd.size());
        td.addAll(insertTd);
    }

    private void updateRanker(double eta) {
        IDFWeighter weighter = new IDFWeighter(td.getTweets());
        ranker = new Ranker(clustream, weighter, eta);
    }

}
