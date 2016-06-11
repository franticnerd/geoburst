package graph;

public class NodeTransition {

    int mId; // the target node id, or the source node id
    double mTransitionProbability;

    public NodeTransition(int id, double probability) {
        this.mId = id;
        this.mTransitionProbability = probability;
    }
    
    public int getNodeId() {
    	return mId;
    }
    
    public double getProbability() {
    	return mTransitionProbability;
    }
    
    @Override
    public String toString() {
    	return "<" + mId + "," + mTransitionProbability + ">";
    }
    
};

