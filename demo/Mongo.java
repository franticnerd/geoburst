package demo;

import com.mongodb.*;
import eventweet.EvenTweet;
import geo.GeoTweet;
import geo.Location;
import geo.TweetDatabase;
import graph.Graph;
import hubseek.Detector;
import waveletdetect.WaveletDetect;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by chao on 7/7/15.
 */
public class Mongo {

    String host;
    int port;
    String db;
    String tweetColName;
    String entityColName;
    String expColName;
    String queryColName;
    String vicinityColName;

    DBCollection tweetCol;
    DBCollection entityCol;
    DBCollection expCol;
    DBCollection queryCol;
    DBCollection vicinityCol;

    public Mongo(Map config) throws Exception {
        host = (String) ((Map)config.get("mongo")).get("dns");
        port = (Integer) ((Map)config.get("mongo")).get("port");
        db = (String) ((Map)config.get("mongo")).get("db");
        tweetColName = (String) ((Map)config.get("mongo")).get("clean_tweet_col");
        entityColName = (String) ((Map)config.get("mongo")).get("entity_col");
        expColName = (String) ((Map)config.get("mongo")).get("exp_col");
        queryColName = (String) ((Map)config.get("mongo")).get("query_col");
        vicinityColName = (String) ((Map)config.get("mongo")).get("vicinity_col");

        MongoClient mongoClient = new MongoClient(host, port);
        DB database = mongoClient.getDB(db);
        tweetCol = database.getCollection(tweetColName);
        entityCol = database.getCollection(entityColName);
        expCol = database.getCollection(expColName);
        queryCol = database.getCollection(queryColName);
        vicinityCol = database.getCollection(vicinityColName);
    }

    // write vicnity information for the entity graph
    public void writeVicinity(Graph graph) {
        List<DBObject> docs = new ArrayList<DBObject>();
        for (int nodeId = 0; nodeId < graph.numNode(); nodeId++) {
            BasicDBObject doc = new BasicDBObject().append("ID", new Integer(nodeId).toString());
            List<List<String>> neighbors = new ArrayList<List<String>>();
            for (Map.Entry<Integer, Double> entry : graph.getVicinity(nodeId).entrySet()) {
                List<String> neighbor = new ArrayList<String>();
                neighbor.add(entry.getKey().toString());
                neighbor.add(entry.getValue().toString());
                neighbors.add(neighbor);
            }
            doc.append("Neighbors", neighbors);
            docs.add(doc);
        }
        vicinityCol.insert(docs);
    }

    public void loadVicinity(Graph graph) {
        Map<Integer, Map<Integer, Double>> vicinity = new HashMap<Integer, Map<Integer, Double>>();
        for (DBObject obj : vicinityCol.find()) {
            int nodeId = new Integer((String) obj.get("ID"));
            Map<Integer, Double> neighborMap = new HashMap<Integer, Double>();
            List<List<String>> neighbors = (List<List<String>>) obj.get("Neighbors");
            for (List<String> neighbor : neighbors) {
                int neighborId = new Integer(neighbor.get(0)).intValue();
                double rwr = new Double(neighbor.get(1)).doubleValue();
                neighborMap.put(neighborId, rwr);
            }
            vicinity.put(nodeId, neighborMap);
        }
        graph.setVicinity(vicinity);
    }

    public void dropVicinity() {
        vicinityCol.drop();
    }

    public void writeExp(Detector hubseek, EvenTweet et, WaveletDetect wd) {
        // get current time when finished running the experiments
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        BasicDBObject doc = new BasicDBObject().append("time", dateFormat.format(date));
        // hubseek
        BasicDBObject hubseekStats = hubseek.statsToBSon();
        List<BasicDBObject> hubseekEvents = hubseek.eventsToBSon();
        doc.append("hubseek_events", hubseekEvents).append("hubseek_stats", hubseekStats);
        // eventweet
        BasicDBObject eventweetStats = et == null ? new BasicDBObject() : et.statsToBSon();
        List<BasicDBObject> eventweetEvents = et == null ? new ArrayList<BasicDBObject>() : et.eventsToBSon();
        doc.append("eventweet_events", eventweetEvents).append("eventweet_stats", eventweetStats);
        // wavelet
        BasicDBObject waveletStats = wd == null ? new BasicDBObject() : wd.statsToBSon();
        List<BasicDBObject> waveletEvents = wd == null ? new ArrayList<BasicDBObject>() : wd.eventsToBSon();
        doc.append("wavelet_events", waveletEvents).append("wavelet_stats", waveletStats);
        expCol.insert(doc);
    }

    public void dropExp() {
        expCol.drop();
    }

    public TweetDatabase rangeQueryTweetDB(long startTS, long endTS) {
        BasicDBObject query = new BasicDBObject("timestamp", new BasicDBObject("$gt", startTS).append("$lt", endTS));
        DBCursor cursor = tweetCol.find(query);
        TweetDatabase td = new TweetDatabase();
        while (cursor.hasNext()) {
            DBObject obj = cursor.next();
            Long tweetId = (Long) obj.get("id");
            Long timestamp = (Long) obj.get("timestamp");
            Long userId = (Long) obj.get("user_id");
            double lng = (Double) obj.get("lng");
            double lat = (Double) obj.get("lat");
            Location loc = new Location(lng, lat);
            List<Integer> entityIds = (List<Integer>) obj.get("entities");
            GeoTweet tweet = new GeoTweet(tweetId, userId, timestamp, loc, entityIds);
            td.add(tweet);
        }
        return td;
    }


    public List<Query> loadBatchQueries(Map config) throws Exception {
        int refWindowSize = (Integer) ((Map) config.get("query")).get("refWindowSize");
        int minSup = (Integer) ((Map) config.get("query")).get("minSup");
        List<Query> queries = new ArrayList<Query>();
        List queryList = (List) ((Map) queryCol.findOne()).get("queries");
        for (Object query : queryList) {
            Integer startTS = ((List<Integer>) query).get(0);
            Integer endTS = ((List<Integer>) query).get(1);
            queries.add(new Query(startTS, endTS, refWindowSize, minSup));
        }
        return queries;
    }

    public List<OnlineQuery> loadOnlineQueries(Map config, Mongo mongo) throws Exception {
        List<Query> batchQueries = loadBatchQueries(config);
        int refWindowSize = (Integer) ((Map) config.get("query")).get("refWindowSize");
        int minSup = (Integer) ((Map) config.get("query")).get("minSup");
        List<Integer> updateWindowSizes = (List<Integer>) ((Map) config.get("query")).get("updateWindow");
        List<OnlineQuery> onlineQueries = new ArrayList<OnlineQuery>();
        for (Query batchQuery : batchQueries) {
            for (Integer updateWindowSize : updateWindowSizes) {
                OnlineQuery query = new OnlineQuery(batchQuery.getStartTS(), batchQuery.getEndTS(),
                        refWindowSize, minSup, updateWindowSize);
                query.loadData(mongo);
                onlineQueries.add(query);
            }
        }
        return onlineQueries;
    }

}

//    public void loadVicinity(Graph graph) {
//        List<Map<Integer, Double>> vicinity = new ArrayList<Map<Integer, Double>>();
//        for (int nodeId = 0; nodeId < graph.numNode(); nodeId++) {
//            BasicDBObject query = new BasicDBObject("ID", new Integer(nodeId).toString());
//            Map<Integer, Double> neighborMap = new HashMap<Integer, Double>();
//            List<List<String>> neighbors = (List<List<String>>) vicinityCol.findOne(query).get("Neighbors");
//            for (List<String> neighbor : neighbors) {
//                int neighborId = new Integer(neighbor.get(0)).intValue();
//                double rwr = new Integer(neighbor.get(1)).doubleValue();
//                neighborMap.put(neighborId, rwr);
//            }
//            vicinity.add(neighborMap);
//        }
//        graph.setVicinity(vicinity);
//    }
