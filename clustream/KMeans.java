package clustream;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.*;

public class KMeans{

	int maxIter;
	Random r = new Random();
	RealVector [] prevMean = null;
	RealVector [] mean = null;
	List<Integer> [] clusters = null;

	int nCluster;
	List<RealVector> data = null;
	List<Double> weights = null;

	public KMeans(int maxIter) {
		this.maxIter = maxIter;
		r.setSeed(100);
	}

	public List<Integer> [] cluster(List<RealVector> data, List<Double> weights, int K) {
		initialize(data, weights, K);
		for (int iteration = 0; iteration < maxIter; iteration++) {
			computeMean();
			if( !hasMeanChanged() )
				break;
			assignToClusters();
		}
		return clusters;
	}


	private void initialize(List<RealVector> data, List<Double> weights, int K) {
        // number of clusters
        if(data.size() < K) {
            System.out.println("Warning: fewer data points than number of clusters.");
            this.nCluster = data.size();
        } else {
            nCluster = K;
        }
        // data
		this.data = data;
        // weights
		if (weights != null) {
			this.weights = weights;
		} else {
			// equal weights
			this.weights = new ArrayList<Double>();
			for (int i=0; i<data.size(); i++)
				this.weights.add(1.0);
		}
        // intermediate arrays
		prevMean = new RealVector[K];
		mean = getRandomMean();
        // results
		clusters = new List [K];
		for(int i=0; i<K; i++) {
			clusters[i] = new ArrayList<Integer>();
		}
		assignToClusters();
	}


	private RealVector [] getRandomMean() {
		Set<RealVector> randomPoints = new HashSet<RealVector>();
		int n = data.size();  // Total number of data points.
		int[] completeArray = new int[n];  // The array of indices.
		for(int i=0; i<n; i++) {
			completeArray[i] = i;
		}
		int bound = n;
		while (randomPoints.size() < nCluster) {
			int randNum = r.nextInt(bound); //generate a random integer between 0~bound-1
			randomPoints.add(data.get(randNum));
			completeArray[randNum] = completeArray[ bound-1 ];
			bound --;
		}
		return randomPoints.toArray(new RealVector[randomPoints.size()]);
	}


	private void assignToClusters() {
		for(int i=0; i<clusters.length; i++)
			clusters[i] = new ArrayList<Integer>();
		for(int i=0; i<data.size(); i++) {
			int assignId = getNearestCluster(data.get(i));
			clusters[ assignId ].add(i);  // assign data i
		}
	}


	private int getNearestCluster(RealVector p) {
		int result = 0;
		double minDist = mean[0].getDistance(p);
		for(int i=1; i<nCluster; i++) {
			double dist = mean[i].getDistance(p);
			if(dist <= minDist) {
				minDist = dist;
				result = i;
			}
		}
		return result;
	}


	private void computeMean() {
		// before computing, store current version of mean into previous mean
		for(int i=0; i<nCluster; i++)
			prevMean[i] = mean[i].copy();
		for(int i=0; i<nCluster; i++) {
			double sumWeight = 0;
			mean[i] = new ArrayRealVector( prevMean[i].getDimension() );
			List<Integer> dataIds = clusters[i];
			for(Integer dataId : dataIds) {
				double weight = weights.get(dataId);
				mean[i] = mean[i].add(data.get(dataId).mapMultiply(weight));
				sumWeight += weight;
			}
			mean[i].mapDivideToSelf(sumWeight);
		}
	}


	private boolean hasMeanChanged() {
		for(int i=0; i<nCluster; i++) {
			if(!prevMean[i].equals(mean[i]))
				return true;
		}
		return false;
	}

	public static void main(String [] args) {
		RealVector rv1 = new ArrayRealVector(new double []{10, -5});
		RealVector rv2 = new ArrayRealVector(new double []{10, -5});
		RealVector rv3 = new ArrayRealVector(new double []{10, -5});
		RealVector rv4 = new ArrayRealVector(new double []{10, -4});
		List<RealVector> data = new ArrayList<RealVector>();
		data.add(rv1);
		data.add(rv2);
		data.add(rv3);
		data.add(rv4);
		KMeans kMeans = new KMeans(100);
		List<Integer> [] results = kMeans.cluster(data, null, 2);
		for (int i=0; i<results.length; i++) {
			System.out.println(results[i]);
		}
	}

}
