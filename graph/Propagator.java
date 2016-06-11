package graph;

import java.util.*;

/**
 * Created by chao on 7/3/15.
 */
public class Propagator {

	Graph mGraph;
	int mGraphSize; // number of nodes in the graph
	double[] mScore; // the approximte scores for all the nodes
	double[] propagationScore; // the max heap that stores the propagation scores of nodes
	PriorityQueue<Integer> queue; // the max heap that stores the propagation scores of nodes

	public Propagator(Graph graph) {
		this.mGraph = graph;
		this.mGraphSize = graph.getNodeCnt();
	}

	public Map<Integer, Double> search(int queryId, double epsilon, double c, double errorBound) {
		initialize(queryId, c);
		int iter = 0;
		while (queue.size() > 0 && propagationScore[queue.peek()] > c * errorBound) {
			int nodeId = queue.poll();
			propagate(nodeId, c);
			propagationScore[nodeId] = 0;
			iter ++;
			if (iter >= 2000) {
				System.out.println(iter);
				break;
			}
		}
		Map<Integer, Double> results = new HashMap<Integer, Double>();
		for (int i = 0; i < mScore.length; i++) {
			if (mScore[i] >= epsilon)
				results.put(i, mScore[i]);
		}
//		System.out.println(results);
		return results;
	}

	private void initialize(int queryId, double c) {
		mScore = new double[mGraphSize];
		propagationScore = new double[mGraphSize];
		mScore[queryId] = propagationScore[queryId] = c;

		queue = new PriorityQueue(100, new Comparator<Integer>() {
			public int compare(Integer c1, Integer c2) {
				// descending order of score
				if (propagationScore[c1] - propagationScore[c2] < 0)
					return 1;
				else if (propagationScore[c1] - propagationScore[c2] > 0)
					return -1;
				else
					return 0;
			}
		}
		);
		queue.offer(queryId);
	}

	// propagate the delta rwr from node id to its in-neighbors
	private void propagate(int nodeId, double c) {
		double toPropagateScore = propagationScore[nodeId];
		List<NodeTransition> inTransitions = mGraph.getIncomingTransitions(nodeId);
		for (NodeTransition nt : inTransitions) {
			int neighborId = nt.getNodeId();
			// compute the delta score for the neighbor
			double probability = nt.getProbability();
			double deltaScore = (1 - c) * probability * toPropagateScore;
			mScore[neighborId] += deltaScore;
			// update the propagation score for the neighbor
			propagationScore[neighborId] += deltaScore;
			// update the heap
			if (queue.contains(neighborId))
				queue.remove(neighborId);
			queue.offer(neighborId);
		}
	}

}
