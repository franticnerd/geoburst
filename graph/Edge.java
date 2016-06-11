package graph;

public class Edge {
	
	int mEdgeId;
    int mFromId;
    int mToId;
    int mWeight;
    static int mAssignEdgeId = 0;

    public Edge(int fromId, int toId, int weight) {
        this.mFromId = fromId;
        this.mToId = toId;
        this.mWeight = weight;
        this.mEdgeId = mAssignEdgeId ++;
    }
    
    public int getEdgeId() {
    	return mEdgeId;
    }
    
    public int getFromId() {
    	return mFromId;
    }
    
    public int getToId() {
    	return mToId;
    }
    
    public int getWeight() {
    	return mWeight;
    }
    
}
