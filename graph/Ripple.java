package graph;

import java.util.*;


public class Ripple {

    Graph mGraph;
    int mGraphSize; // number of nodes in the graph
    int S = 1; // the number of nodes for expansion in each round
    int T = 1; // the number of iterations after expansion in each round

    Double[] mScore = null;
    Double[] mPreviousScore = null;
    Set<Integer> mVicinityNodes = null;
    Set<Integer> mBoundaryNodes = null;

    double mOutsideUB = 1.0; // upper bound for nodes outside the neighborhood
    double mInsideUB = 1.0; // upper bound for nodes inside the neighborhood

    int mTotalIterationNum = 0;

    double mDelta = 0; // the change of score after the first iteration
    double mConstant = 0; // (1-c)^T, pre-store to avoid repeatedly computing


    public Ripple(Graph graph) {
        this.mGraph = graph;
        this.mGraphSize = graph.getNodeCnt();
    }

    public Map<Integer, Double> search(int queryId, double epsilon, double c, double errorBound) {
        initialize(queryId, c);
        while (true) {
            if( mOutsideUB <= errorBound || mTotalIterationNum >= 50)
                break;
            if (mBoundaryNodes.size() > 0)
                expand();
            update(queryId, c);
            calcBounds(c);
        }
        Map<Integer, Double> results = new HashMap<Integer, Double>();
        for(int i=0; i<mScore.length; i++) {
            if (mScore[i] >= epsilon)
                results.put(i, mScore[i]);
        }
//        System.out.println(results);
        return results;
    }

    private void initialize(int queryId, double c) {
        // initialize the constant
        mConstant = Math.pow(1 - c, T);
        mDelta = 0; // the change of score after the first iteration
        mTotalIterationNum = 0;
        // initialize the bounds
        mInsideUB = mOutsideUB = 1.0;

        // initialize the scores
        mScore = new Double[mGraphSize];
        mPreviousScore = new Double[mGraphSize];
        for (int i = 0; i < mGraphSize; i++)
            mScore[i] = mPreviousScore[i] = 0.0;

        // initialize boundary and neighborhood
        mVicinityNodes = new HashSet<Integer>();
        mBoundaryNodes = new HashSet<Integer>();

        mVicinityNodes.add(queryId);
        mBoundaryNodes.add(queryId);
    }


    private void expand() {
        // expand S boundary nodes with the largest score
        int boundaryNodeId = findToExpandBoundaryNode();
        mBoundaryNodes.remove(boundaryNodeId);
        ArrayList<Integer> inNeighbors = mGraph.getInNeighbors(boundaryNodeId);
        mVicinityNodes.addAll(inNeighbors); // update the vicinity node set
        mBoundaryNodes.addAll(inNeighbors); // update the boundary node set, false positives will be removed later

        // update the boundary node set
        Iterator<Integer> iterator = mBoundaryNodes.iterator();
        while (iterator.hasNext()) {
            Integer nodeId = iterator.next();
            if (!isBoundary(nodeId)) {
                iterator.remove(); // remove false positive
            }
        }
    }

    // find 1 node that needs to be expanded
    private int findToExpandBoundaryNode() {
        int bestBoundaryNodeId = 0;
        double maxBoundaryScore = 0;
        for (Integer boundaryNodeId : mBoundaryNodes) {
            if(mScore[boundaryNodeId] > maxBoundaryScore) {
                maxBoundaryScore = mScore[boundaryNodeId];
                bestBoundaryNodeId = boundaryNodeId;
            }
        }
        return bestBoundaryNodeId;
    }

    private boolean isBoundary(int id) {
        List<Integer> inNeighbors = mGraph.getInNeighbors(id);
        for (Integer i : inNeighbors) {
            if (!mVicinityNodes.contains(i))
                return true;
        }
        return false;
    }


    // swap previous and current score arrays, preparing for update
    private void swap() {
        Double[] tmp = mPreviousScore;
        mPreviousScore = mScore;
        mScore = tmp;
    }


    // update the scores for nodes in the vicinity
    public void update(int queryId, double c) {
        for (int i = 1; i <= T; i++) {
            swap(); // swap previous and current score arrays, preparing for update
            for (Integer nodeId : mVicinityNodes) {
                List<NodeTransition> outTransitions = mGraph.getOutgoingTransitions(nodeId);
                double score = 0;
                for (NodeTransition nt : outTransitions) {
                    double probability = nt.getProbability();
                    int neighborId = nt.getNodeId();
                    score += mPreviousScore[neighborId] * probability;
                }
                score *= (1 - c);
                // the query node itself
                if (nodeId == queryId) {
                    score += c;
                }
                mScore[nodeId] = score;
            }

            if (i == 1) {
                findDelta();
            }
        }
    }

    private void findDelta() {
        mDelta = 0;
        for (Integer nodeId : mVicinityNodes)
            if (mScore[nodeId] - mPreviousScore[nodeId] > mDelta)
                mDelta = mScore[nodeId] - mPreviousScore[nodeId];
    }


    private void calcBounds(double c) {
        // calc maximum boundary node score
        double maxBoundaryScore = 0;
        for (Integer i : mBoundaryNodes)
            if (mScore[i] > maxBoundaryScore)
                maxBoundaryScore = mScore[i];

        mOutsideUB = (1 - c) * (maxBoundaryScore + mConstant * mDelta / c) / (2 * c - c * c);
        mInsideUB = (1 - c) * (1 - c) / (2 * c - c * c) * maxBoundaryScore + mConstant * mDelta / (2 * c * c - c * c * c);
    }

}