package graph;

import java.util.*;

public class Squeeze {

    Graph mGraph;
    int mGraphSize; // number of nodes in the graph
	Double [] mScore = null;
	Double [] mPreviousScore = null;
	double mErrorBound;
	double mThreshold = -1; // the k-th lower bound as the threshold
	int mTotalIter = 0;

	public Squeeze(Graph graph) {
        this.mGraph = graph;
        this.mGraphSize = graph.getNodeCnt();
	}
	
	public Map<Integer, Double> search(int queryId, double epsilon, double c, double errorBound) {
        initialize();
        while (mErrorBound >= errorBound) {
			swap();
			update(queryId, c);
            mTotalIter += 1;
			mErrorBound *= (1-c);
		}
        Map<Integer, Double> results = new HashMap<Integer, Double>();
        for(int i=0; i<mScore.length; i++) {
            if (mScore[i] >= epsilon)
                results.put(i, mScore[i]);
        }
//		System.out.println(results);
		return results;
	}
	
	public void initialize() {
        mErrorBound = 1.0;
        mThreshold = -1.0;
        mTotalIter = 0;
		mScore = new Double [mGraphSize];
		mPreviousScore = new Double [mGraphSize];
		for(int i=0; i<mGraphSize; i++)
			mScore[i] = mPreviousScore[i] = 0.0;
	}
	
	public void update(int queryId, double c) {
		for(int i=0; i<mGraphSize; i++)	{
			List<NodeTransition> outNeighbors = mGraph.getOutgoingTransitions(i);
			double score = 0;
			for(NodeTransition nt: outNeighbors) {
				double probability = nt.getProbability();
				int neighborId = nt.getNodeId();
				score += mPreviousScore[neighborId] * probability;
			}
			score *= (1 - c);
			// the query node itself
			if( i == queryId )	{
				score += c;
			}
			mScore[i] = score;
		}
	}
	
	//swap previous and current socialInfluenceArray, preparing for next iteration
	private void swap()	{
		Double [] tmp = mPreviousScore;
		mPreviousScore = mScore;
		mScore = tmp;
	}
	
    public int getNumOfIter() {
        return mTotalIter;
    }

	public static void main(String[] args) throws Exception {
		String dataDir = "/Users/chao/Dataset/Twitter/";
		String nodeFile = dataDir + "entities.txt";
		String edgeFile = dataDir + "entity_edges.txt";
		Graph graph = new Graph();
		graph.loadNodes(nodeFile);
		graph.loadEdges(edgeFile, false);
		Squeeze searcher = new Squeeze(graph);
		Map<Integer, Double> results = searcher.search(673, 0.05, 0.2, 1e-3);
		System.out.println(results);
	}
}