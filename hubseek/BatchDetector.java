package hubseek;

import clustream.Clustream;
import geo.GeoTweet;
import geo.TweetDatabase;
import graph.Graph;
import rank.IDFWeighter;
import rank.Ranker;
import utils.ScoreCell;

import java.util.ArrayList;
import java.util.List;


/**
 * This class detects the events in a batch mode.
 * Created by chao on 7/7/15.
 */
public class BatchDetector extends Detector {

    public BatchDetector(Clustream clustream, Graph graph) {
        super(clustream, graph);
    }

    public void detect(TweetDatabase td, long queryInterval, double bandwidth, double epsilon, double minSup, long refTimeSpan, double eta) {
        // init
        init(td, queryInterval, bandwidth, epsilon, eta);
        // use hubseek to get the clusters
        List<TweetCluster> clusters = hubseek(minSup);
        System.out.println("Hubseek done generating candidates.");
        // rank the clusters as events
        events = rank(clusters, bandwidth, refTimeSpan);
        System.out.println("Hubseek done ranking the events.");
        // set the stats
        setStats();
    }

    // init the workers
    protected void init(TweetDatabase td, long queryInterval, double bandwidth, double epsilon, double eta) {
        this.td = td;
        hubSeek = new HubSeek(bandwidth, epsilon, graph);
        IDFWeighter weighter = new IDFWeighter(td.getTweets());
        ranker = new Ranker(clustream, weighter, eta);
        this.bandwidth = bandwidth;
        this.epsilon = epsilon;
        this.eta = eta;
        this.startTS = td.getStartTimestamp();
        this.endTS = td.getEndTimestamp();
        this.queryInterval = queryInterval;
    }

    // get the candiadate events using hubseek.
    protected List<TweetCluster> hubseek(double minSup) {
        List<GeoTweet> tweets = td.getTweets();
        hubSeek.cluster(tweets);
        return hubSeek.genClusters(minSup);
    }

    // rank the clusters with the background knowledge in clustream.
    protected List<TweetCluster> rank(List<TweetCluster> clusters, double bandwidth, long refTimeSpan) {
        // get the ranking list for the clusters
        List<ScoreCell> scoreCells = ranker.rank(clusters, bandwidth, td.getStartTimestamp(), td.getEndTimestamp(), refTimeSpan);
        // organize the clusters into a ranked order.
        List<TweetCluster> sortedClusters = new ArrayList<TweetCluster>();
        for (ScoreCell sc : scoreCells) {
            int clusterIndex = sc.getId();
            sortedClusters.add(clusters.get(clusterIndex));
        }
        return sortedClusters;
    }

    // not used for batch detector
    public void update(TweetDatabase deleteTweets, TweetDatabase insertTweets, double bandwidth, double minSup, long refTimeSpan, double eta) {}
}
