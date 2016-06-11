package hubseek;

import clustream.Clustream;
import com.mongodb.BasicDBObject;
import geo.TweetDatabase;
import graph.Graph;
import rank.Ranker;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chao on 7/7/15.
 */
public abstract class Detector {

    // main components
    Clustream clustream;
    Graph graph;
    HubSeek hubSeek;
    Ranker ranker;

    TweetDatabase td;

    // parameters
    double bandwidth;
    double epsilon;
    double eta;
    long startTS;
    long endTS;
    boolean updateMode = false;

    // each event is actually a geo-entity cluster
    List<TweetCluster> events;

    // stats
    long queryInterval;
    int numEvents; // the final number of detected events
    int numTweetsInClustream; // total number of tweets that have been processed by clustream
    int numTweetsHubSeek; // number of tweets in the current window.
    int numTweetsHubSeekDeletion; // number of tweets that have been deleted.
    int numTweetsHubSeekInsertion; // number of tweets that have been inserted.
    int numReferencePeriods; // number of reference periods when ranking.
    double timeClustream; // elapsed time for clustream
    double timeGraphVicinity; // time for computing vicinity in graph.
    double timeHubSeekBatch; // elapsed time for clustering the tweets in a batch mode.
    double timeHubSeekDeletion; // elapsed time for deletion
    double timeHubSeekInsertion; // elapsed time for insertion.

    public Detector(Clustream clustream, Graph graph) {
        this.clustream = clustream;
        this.graph = graph;
    }

    public abstract void detect(TweetDatabase td, long queryInterval, double bandwidth, double epsilon, double minSup, long refTimeSpan, double eta);

    public abstract void update(TweetDatabase deleteTweets, TweetDatabase insertTweets,
                                double bandwidth, double minSup, long refTimeSpan, double eta);

    /**
     * Stat info.
     */

    protected void setStats() {
        this.numEvents = events.size();
        this.numTweetsInClustream = clustream.getNumOfProcessedTweets();
        this.numTweetsHubSeek = hubSeek.getNumBatchTweets();
        this.numTweetsHubSeekDeletion = hubSeek.getNumDeletedTweet();
        this.numTweetsHubSeekInsertion = hubSeek.getNumInsertedTweet();
        this.numReferencePeriods = ranker.getNumReferencePeriods();
        this.timeClustream = clustream.getElapsedTime();
        this.timeGraphVicinity = graph.getTimeCalcVicinity();
        this.timeHubSeekBatch = hubSeek.getTimeBatchClustering();
        this.timeHubSeekDeletion = hubSeek.getTimeDeletion();
        this.timeHubSeekInsertion = hubSeek.getTimeInsertion();
    }

    public void printStats() {
        clustream.printStat();
        hubSeek.printStat();
        ranker.printStats();
    }

    public void writeEvents(String clusterFile) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(clusterFile, true));
        for(TweetCluster gec : events) {
            bw.append(gec.toString());
        }
        bw.close();
    }

    public void writeStats(String statFile) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(statFile, true));
        bw.write(statsToBSon().toString() + "\n");
        bw.close();
    }

    public BasicDBObject statsToBSon() {
        return new BasicDBObject()
                .append("numEvents", numEvents)
                .append("numTweetsInClustream", numTweetsInClustream)
                .append("numTweetsHubSeek", numTweetsHubSeek)
                .append("numTweetsDeletion", numTweetsHubSeekDeletion)
                .append("numTweetsInsertion", numTweetsHubSeekInsertion)
                .append("timeClustream", timeClustream)
                .append("timeGraphVicinity", timeGraphVicinity)
                .append("timeHubSeekBatch", timeHubSeekBatch)
                .append("timeHubSeekDeletion", timeHubSeekDeletion)
                .append("timeHubSeekInsertion", timeHubSeekInsertion)
                .append("bandwidth", bandwidth)
                .append("epsilon", epsilon)
                .append("eta", eta)
                .append("startTS", startTS)
                .append("endTS", endTS)
                .append("update", updateMode)
                .append("queryInterval", queryInterval);
    }


    public List<BasicDBObject> eventsToBSon() {
        List<BasicDBObject> ret = new ArrayList<BasicDBObject>();
        for (int i = 0; i < events.size(); i++) {
            TweetCluster event = events.get(i);
            ret.add(event.toBSon());
        }
        return ret;
    }

}
