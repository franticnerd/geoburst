package geo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

/**
 * This class keeps the tweets in a specific time span.
 * Created by chao on 6/20/15.
 */
public class TweetDatabase {

    List<GeoTweet> tweets = new ArrayList<GeoTweet>();
    long startTimestamp = Long.MAX_VALUE;
    long endTimestamp = Long.MIN_VALUE;
    List<Long> userIds = new ArrayList<Long>();

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public List<GeoTweet> getTweets() {
        return tweets;
    }

    // get one tweet by index.
    public GeoTweet getTweet(int index) {
        return tweets.get(index);
    }

    public void load(String tweetFile) throws Exception {
        BufferedReader br = new BufferedReader( new FileReader(tweetFile) );
        while(true) {
            String line = br.readLine();
            if (line == null) break;
            GeoTweet gt = new GeoTweet(line);
            tweets.add(gt);
        }
        br.close();
    }

    // delete the first #num tweets in the database
    public void deleteFromHead(int num) {
        if (tweets.size() - num <= 0)
            return;
        startTimestamp = tweets.get(num).getTimestamp();
        List<GeoTweet> updatedTweets = new ArrayList<GeoTweet>();
        for (int i=num; i<tweets.size(); i++) {
            updatedTweets.add(tweets.get(i));
        }
        tweets = updatedTweets;
    }

    // delete the first #num tweets in the database until the first tweet is newer than startTS.
    public void deleteFromHead(long startTS) {
        List<GeoTweet> updatedTweets = new ArrayList<GeoTweet>();
        for (int i=0; i<tweets.size(); i++) {
            GeoTweet t = tweets.get(i);
            if (t.getTimestamp() > startTS)
                updatedTweets.add(t);
        }
        tweets = updatedTweets;
    }

    // append the new tweets to the end
    public void addAll(TweetDatabase td) {
        tweets.addAll(td.getTweets());
        endTimestamp = td.getEndTimestamp();
    }

    public void add(GeoTweet tweet) {
        tweets.add(tweet);
        userIds.add(tweet.getUserId());
        if (tweet.getTimestamp() < startTimestamp)
            startTimestamp = tweet.getTimestamp();
        if (tweet.getTimestamp() > endTimestamp)
            endTimestamp = tweet.getTimestamp();
    }

    public boolean containUserId(GeoTweet tweet) {
        return userIds.contains(tweet.getUserId());
    }

    public Map<Long, GeoTweet> getGeoTweetsMap() {
        Map<Long, GeoTweet> results = new HashMap<Long, GeoTweet>();
        for(GeoTweet gt : tweets) {
            results.put(gt.getTweetId(), gt);
        }
        return results;
    }

    public int size() {
        return tweets.size();
    }
}
