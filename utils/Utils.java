package utils;

import java.util.List;
import java.util.Random;

/**
 * Created by chao on 6/20/15.
 */
public class Utils {

    static Random r = new Random(100);

    /*******************  Basic aggregate functions *********************/
    // Find the max value of an array.
    public static double max(double [] data) {
        if (data.length == 0) {
            System.out.println("Error when finding the max value. Array length is 0!");
            System.exit(1);
        }
        double maxValue = data[0];
        for (int i=0; i<data.length; i++) {
            if (data[i] > maxValue)
                maxValue = data[i];
        }
        return maxValue;
    }

    // Find the sum of the array
    public static double sum(double [] data) {
        if (data.length == 0) {
            System.out.println("Error when finding the sum. Array length is 0!");
            System.exit(1);
        }
        double sumValue = 0;
        for (int i=0; i<data.length; i++) {
            sumValue += data[i];
        }
        return sumValue;
    }

    // Find the sum of the list
    public static double sum(List<Double> data) {
        if (data.size() == 0) {
            System.out.println("Error when finding the sum. List size is 0!");
            System.exit(1);
        }
        double sumValue = 0;
        for (int i=0; i<data.size(); i++) {
            sumValue += data.get(i);
        }
        return sumValue;
    }

    /*******************  For log operations *********************/
    // Find the sum of exp of the array
    public static double expSum(double [] data) {
        double sumValue = 0;
        for (int i=0; i<data.length; i++) {
            sumValue += Math.exp(data[i]);
        }
        return sumValue;
    }

    // Find the sum of exp of the array, and then take the log
    public static double sumExpLog(double [] data) {
        double maxValue = max(data);
        double sumValue = 0;
        for (int i=0; i<data.length; i++)
            sumValue += Math.exp(data[i] - maxValue);
        return Math.log(sumValue) + maxValue;
    }

    // Normalize the array
    public static void normalize(double [] data) {
        if (data.length == 0) {
            System.out.println("Error when normalizing. Array length is 0!");
            System.exit(1);
        }
        double sumValue = sum(data);
        if (sumValue == 0) {
            System.out.println("Warning: sum of the elements is 0 when normalizing!");
            return;
        }
        for (int i=0; i<data.length; i++)
            data[i] /= sumValue;
    }

    // Input: an array in the log domain; Output: the ratio in the exp domain
    public static void logNormalize(double[] data) {
        if (data.length == 0) {
            System.out.println("Error when doing log-sum-exp. Array length is 0!");
            System.exit(1);
        }
        double maxValue = max(data);
        for (int i=0; i<data.length; i++)
            data[i] = Math.exp(data[i] - maxValue);
        normalize(data);
    }

    // calc accuracy
    public static double calcAccuracy(List<Integer> groundTruth, List<Integer> predicted) {
        if (groundTruth.size() != predicted.size()) {
            System.out.println("Error, the ground truth and predicted data do not have equal length!");
            System.exit(1);
        }
        System.out.println(groundTruth);
        System.out.println(predicted);
        int denominator = groundTruth.size();
        int numerator = 0;
        for (int i=0; i<groundTruth.size(); i++) {
            if (groundTruth.get(i).intValue() == predicted.get(i).intValue())
                numerator += 1;
        }
        return (double) numerator / (double) denominator;
    }


    // Generate K distinct random numbers in [0,n-1]
    public static int[] genKRandomNumbers(int n, int k) {
        int[] completeArray = new int[n];
        for(int i=0; i<n; i++) {
            completeArray[i] = i;
        }
        int[] result = new int[k];
        int bound = n;
        for(int i=0; i<k; i++) {
            int randNum = r.nextInt( bound ); //generate a random integer between 0~bound-1
            result[i] = completeArray[ randNum ];
            completeArray[randNum] = completeArray[ bound-1 ];
            completeArray[bound-1] = result[i];
            bound --;
        }
        return result;
    }

    /*******************  For normal distributions *********************/
    public static double mean(List<Double> data) {
        return sum(data) / data.size();
    }

    public static double std(List<Double> data) {
        double squareSum = 0;
        for (Double v : data)
            squareSum += v*v;
        double m = mean(data);
        return Math.sqrt(squareSum / data.size() - m*m);
    }

    /*******************  For normal distributions *********************/
    // get the quantiles for a normal distribution.
	public static double getQuantile(double z) {
		assert( z >= 0 && z <= 1 );
		return Math.sqrt( 2 ) * inverseError( 2*z - 1 );
	}

	private static double inverseError(double x) {
		double z = Math.sqrt(Math.PI) * x;
		double res = (z) / 2;

		double z2 = z * z;
		double zProd = z * z2; // z^3
		res += (1.0 / 24) * zProd;

		zProd *= z2;  // z^5
		res += (7.0 / 960) * zProd;

		zProd *= z2;  // z^7
		res += (127 * zProd) / 80640;

		zProd *= z2;  // z^9
		res += (4369 * zProd) / 11612160;

		zProd *= z2;  // z^11
		res += (34807 * zProd) / 364953600;

		zProd *= z2;  // z^13
		res += (20036983 * zProd) / 797058662400d;

		return res;
	}

}
