package demo;

/**
 * Created by chao on 9/15/15.
 * Local Event Query.
 */
public class Query {

    long startTS; // start timestamp
    long endTS; // end timestamp
    long startRefTS; // start reference timestamp
    long endRefTS; // end reference timestamp
    int minSup; // minimum support for the result event

    public Query(long start, long end, long refWindowSize, int minSup) {
        this.startTS = start;
        this.endTS = end;
        this.startRefTS = startTS - refWindowSize;
        this.endRefTS = startTS;
        this.minSup = minSup;
    }

    public long getStartTS() {
        return startTS;
    }

    public long getEndTS() {
        return endTS;
    }

    public long getRefStartTS() {
        return startRefTS;
    }

    public long getRefEndTS() {
        return endRefTS;
    }

    public int getMinSup() {
        return minSup;
    }

    public long getQueryInterval() {
        return endTS - startTS;
    }

}
