package demo;

import geo.TweetDatabase;

/**
 * Created by chao on 9/15/15.
 */

public class OnlineQuery extends Query {

    TweetDatabase batchTD;
    TweetDatabase deleteTD;
    TweetDatabase insertTD;

    long startDeleteTS;
    long endDeleteTS;
    long startInsertTS;
    long endInsertTS;

    public OnlineQuery(long start, long end, long refWindowSize, int minSup, int updateWindow) {
        super(start, end, refWindowSize, minSup);
        startDeleteTS = startTS;
        endDeleteTS = startTS + updateWindow;
        startInsertTS = endTS;
        endInsertTS = endTS + updateWindow;
    }

    public void loadData(Mongo mongo) {
        batchTD = mongo.rangeQueryTweetDB(startTS, endTS);
        deleteTD = mongo.rangeQueryTweetDB(startDeleteTS, endDeleteTS);
        insertTD = mongo.rangeQueryTweetDB(startInsertTS, endInsertTS);
        System.out.println("batch size" + batchTD.size() + "delete size:" + deleteTD.size() + "insert size:" + insertTD.size());
    }

    public TweetDatabase getBatchTD() {
        return batchTD;
    }

    public TweetDatabase getDeleteTD() {
        return deleteTD;
    }

    public TweetDatabase getInsertTD() {
        return insertTD;
    }

}
