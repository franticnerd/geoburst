package graph;

public class Node {
    int mId;
    double mWeight;
    String mDescription;
    int mOutDegree;

    public Node(int id, double weight, String description) {
        this.mId = id;
        this.mWeight = weight;
        this.mDescription = description;
        this.mOutDegree = 0;
    }

    public int getId() {
    	return mId;
    }

    public double getWeight() {
        return mWeight;
    }
    
    public String getDescription() {
    	return mDescription;
    }
    
    public int getOutDegree() {
    	return mOutDegree;
    }
    
    public void updateOutDegree(int degree) {
        mOutDegree += degree;
    }

};

