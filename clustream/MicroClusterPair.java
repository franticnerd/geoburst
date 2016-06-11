package clustream;

public class MicroClusterPair {
	
	MicroCluster clusterA;
	MicroCluster clusterB;
	double dist;

	public MicroClusterPair(MicroCluster cluster1, MicroCluster cluster2, double dist) {
		this.clusterA = cluster1;
		this.clusterB = cluster2;
		this.dist = dist;
	}


	public double getDist() {
		return dist;
	}

}
