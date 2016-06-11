package graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Graph {

    public List<Node> mNodes = new ArrayList<Node>();
    public List<Edge> mEdges = new ArrayList<Edge>();
    // keep the vicinity for each node.
	public Map<Integer, Map<Integer, Double>> vicinity = new HashMap<Integer, Map<Integer, Double>>();
    public List< Set<Integer> > mOutEdges = new ArrayList< Set<Integer> >(); // the outgoing edges of nodes
    public List< Set<Integer> > mInEdges = new ArrayList< Set<Integer> >(); // the incoming edges of nodes

    // the row-normalized transition matrix
    ArrayList< ArrayList<NodeTransition> > mTransition = new ArrayList< ArrayList<NodeTransition>> ();
    
    // the transpose of the row-normalized transition matrix
    ArrayList< ArrayList<NodeTransition> > mTransitionTranspose = new ArrayList< ArrayList<NodeTransition>> ();

    // stats
    double timeCalcVicinity; // time for computing vicinity.

    /**
     * Format of each line: node ID, weight, node description
     */
    public void loadNodes(String nodeFile) throws Exception {
    	BufferedReader br = new BufferedReader( new FileReader(nodeFile) );
        while(true)  {
            String line = br.readLine();
            if(line == null)
            	break;
            Scanner sr = new Scanner(line);
            if(!sr.hasNextLine())
            	break;
            sr.useDelimiter("\\t");
            int id = sr.nextInt(); // node id
            double weight = 1.0;
            String description = sr.next(); // description
            mNodes.add( new Node(id, weight, description) );
        }
        br.close();
        System.out.println("Loading graph nodes completed. Number of nodes:" + getNodeCnt() );
        // initialize empty neighbor sets for each node
        for (int i = 0; i < getNodeCnt(); i++) {
        	Set<Integer> outEdges = new HashSet<Integer> ();
			Set<Integer> inEdges = new HashSet<Integer> ();
			mOutEdges.add( outEdges );
			mInEdges.add( inEdges );
		}
        
    }
	
    public void loadEdges(String edgeFile, boolean directed) throws Exception {
		BufferedReader br = new BufferedReader( new FileReader(edgeFile) );
        while(true)  {
            String line = br.readLine();
            if(line == null)
            	break;
            Scanner sr = new Scanner(line);
            if(!sr.hasNextLine())
            	break;
            sr.useDelimiter("\\t");
            int fromId = sr.nextInt();
            int toId = sr.nextInt();
            int weight = sr.hasNextInt() ? sr.nextInt() : 1;
            Edge edge = new Edge(fromId, toId, weight);
            addEdge( edge );
            // for undirected graph
            if(directed == false) {
            	Edge reverseEdge = new Edge(toId, fromId, weight);
            	addEdge(reverseEdge);
            }
        }
        br.close();
        System.out.println("Loading edges completed. Number of edges:" + getEdgeCnt() );
        constructTransitionMatrix();
        constructTransitionTransposeMatrix();
        System.out.println("Constructing transition matrices completed.");
	}
    
    private void addEdge(Edge edge) {
    	mEdges.add(edge);
    	// udpate the incoming and outgoing neighbors of nodes
    	int edgeId = edge.getEdgeId();
    	mOutEdges.get( edge.getFromId() ).add(edgeId);
    	mInEdges.get( edge.getToId() ).add(edgeId);
    	// update the degree information of nodes
    	mNodes.get( edge.getFromId() ).updateOutDegree(edge.getWeight());
    }
    
    public Node getNode(int nodeId) {
    	return mNodes.get( nodeId );
    }
    
    public Edge getEdge(int edgeId) {
    	return mEdges.get( edgeId );
    }   
    
    public int getNodeCnt() {
    	return mNodes.size();
    }
    
    public int getEdgeCnt() {
    	return mEdges.size();
    }
    
    public Set<Integer> getOutEdges(int nodeId) {
    	return mOutEdges.get(nodeId);
    }
    
    public Set<Integer> getInEdges(int nodeId) {
    	return mInEdges.get(nodeId);
    }
    
    public int getOutDegree(int nodeId) {
    	int degree = 0;
    	Set<Integer> edgeIds = mOutEdges.get( nodeId );
    	for(Integer edgeId : edgeIds) 
    		degree += mEdges.get(edgeId).getWeight();
    	return degree;
    }
    
    public int getInDegree(int nodeId) {
    	int degree = 0;
    	Set<Integer> edgeIds = mInEdges.get( nodeId );
    	for(Integer edgeId : edgeIds) 
    		degree += mEdges.get(edgeId).getWeight();
    	return degree;
    }
    
    // get the outgoing neighbors of a given node
	public ArrayList<Integer> getOutNeighbors (int nodeId) {
		ArrayList<Integer> outNeighbors = new ArrayList<Integer>();
		// the set of outgoing edge ids
		Set<Integer> edgeSet = mOutEdges.get(nodeId);
		for(Integer edgeId: edgeSet) {
			outNeighbors.add( getEdge(edgeId).getToId() );
		}
		return outNeighbors;
	}
	
	// get the incoming neighbors of a given node
	public ArrayList<Integer> getInNeighbors (int nodeId) {
		ArrayList<Integer> inNeighbors = new ArrayList<Integer>();
		// the set of incoming edge ids
		Set<Integer> edgeSet = mInEdges.get(nodeId);
		for(Integer edgeId: edgeSet) {
			inNeighbors.add( getEdge(edgeId).getFromId() );
		}
		return inNeighbors;
	}
	
	public List<NodeTransition> getOutgoingTransitions(int nodeId) {
		return mTransition.get( nodeId );
	}
	
	public List<NodeTransition> getIncomingTransitions(int nodeId) {
		return mTransitionTranspose.get( nodeId );
	}
	

	// construct the row-normalized transition matrix
	private void constructTransitionMatrix() {
		for(Node node: mNodes) {
			int nodeId = node.getId();
			int outDegree = node.getOutDegree();
			ArrayList<NodeTransition> transitionList = new ArrayList<NodeTransition>();
			
			Set<Integer> outEdges = getOutEdges(nodeId);
			for(Integer edgeId: outEdges) {
				int toId = getEdge(edgeId).getToId();
				double pTransition = (double) getEdge(edgeId).getWeight() / (double) outDegree;
				transitionList.add( new NodeTransition(toId, pTransition) );
			}
			mTransition.add( transitionList );
		}
	}
	
	// construct the transpose of the row-normalized transition matrix
	// it answers: given a node, which other node can directly walk to it, and with what probabilities?
	private void constructTransitionTransposeMatrix() {
		for(int i=0; i<getNodeCnt(); i++) {
			ArrayList<NodeTransition> transitionList = new ArrayList<NodeTransition>();
			mTransitionTranspose.add( transitionList );
		}
		for(Node node: mNodes) {
			int fromId = node.getId();
			int outDegree = node.getOutDegree();
			
			Set<Integer> outEdges = getOutEdges(fromId);
			for(Integer edgeId: outEdges) {
				int toId = getEdge(edgeId).getToId();
				double pTransition = (double) getEdge(edgeId).getWeight() / (double) outDegree;
				
				ArrayList<NodeTransition> transitionList = mTransitionTranspose.get(toId);
				transitionList.add( new NodeTransition(fromId, pTransition) );
			}
		}
	}


	// compute the rwr scores for all the nodes.
	// epsilon: the rwr threshold; error bound: the rwr error; c: the restart probability
	public void calcVicinity(double epsilon, double errorBound, double c) {
//		Squeeze searcher = new Squeeze(this);
        Propagator searcher = new Propagator(this);
//        Ripple searcher = new Ripple(this);
		int cnt = 0;
        long start = System.currentTimeMillis();
		for (Node n : mNodes) {
			Map<Integer, Double> neighbors = searcher.search(n.getId(), epsilon, c, errorBound);
			vicinity.put(n.getId(), neighbors);
			cnt ++;
			if (cnt % 100 == 0) {
                System.out.println("Finished computing vicinity for " + cnt + " nodes.");
//                break;
            }
		}
        long end = System.currentTimeMillis();
        timeCalcVicinity = (end - start) / 1000.0;
	}


	public double getRWR(int fromId, int toId) {
		Map<Integer, Double> neighbors = vicinity.get(toId);
		if (neighbors.containsKey(fromId))
			return neighbors.get(fromId);
		else
			return 0;
	}

	public Map<Integer, Double> getVicinity(int nodeId) {
		return vicinity.get(nodeId);
	}

    public void setVicinity(Map<Integer, Map<Integer, Double>> vicinity) {
        this.vicinity = vicinity;
    }

	public int numNode() {
		return mNodes.size();
	}

	public int numEdges() {
		return mEdges.size();
	}

    public void printStats() {
        String s = "Graph Stats:";
        s += " numNode=" + numNode();
        s += "; numEdges=" + numEdges();
        s += "; timeCalcVicinity=" + timeCalcVicinity;
        System.out.println(s);
    }

    public double getTimeCalcVicinity() {
        return timeCalcVicinity;
    }

}
