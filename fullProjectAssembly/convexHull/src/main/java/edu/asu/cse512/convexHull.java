package edu.asu.cse512;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class convexHull implements Serializable 
{
	private static final long serialVersionUID = -205075934063918279L;

	class SortPoints implements Function<GeoPoint,GeoPoint>, Serializable
	{
		private static final long serialVersionUID = 8225302338329281442L;

		public GeoPoint call(GeoPoint geoPoint) throws Exception {
			return geoPoint;
		}
	}

	class ParsePoints implements Function<String, GeoPoint>, Serializable
	{
		private static final long serialVersionUID = -3885195256936448019L;

		public GeoPoint call(String inputLine)
		{
			return new GeoPoint(inputLine);
		}
	}

	class CalculateHull implements FlatMapFunction<Iterator<GeoPoint>, GeoPoint>, Serializable
	{
		private static final long serialVersionUID = -4620305040646468710L;

		public Iterable<GeoPoint> call(Iterator<GeoPoint> geoPoints) throws Exception {
			List<GeoPoint> newGeoPoints = new ArrayList<GeoPoint>();

			// Handle the zero points case.
			if (!geoPoints.hasNext()) 
				return newGeoPoints; 

			// First point is always on hull.
			GeoPoint firstPoint = geoPoints.next();
			newGeoPoints.add(firstPoint);

			if (!geoPoints.hasNext())
				return newGeoPoints; 
			GeoPoint secondPoint = geoPoints.next();

			while (geoPoints.hasNext()) {
				GeoPoint thirdPoint = geoPoints.next();

				// If the slope from first to third is less than the slope from first
				//  to second, then use second.  Second becomes basis for next points.
				//  If not, then don't save second, and keep first as basis for next
				//  comparison.
				double firstThirdSlope = firstPoint.getSlope(thirdPoint);
				double firstSecondSlope = firstPoint.getSlope(secondPoint);

				//System.out.println("CalculateHull:  1st= " + firstPoint + " 2nd= " + secondPoint +
				//           " 3rd= " + thirdPoint);
				//System.out.printf("            1-3slope= %10.2f  1-2slope= %10.2f%n", firstThirdSlope, firstSecondSlope );

				boolean	useSecond = false;

				// Handle the special case of vertical points, where slope would be infinite.
				if (firstPoint.x() == secondPoint.x())
				{
					// Only use a vertical pairs of points if there are 3 vertical points in a row.
					if (firstPoint.x() == thirdPoint.x())
						useSecond = true;
				}
				// Only check slopes if firstSecond isn't infinite
				else if (firstThirdSlope <= firstSecondSlope)
					useSecond = true;

				// So now we use the second point, and make the second point the basis for comparison.
				if (useSecond)
				{
					newGeoPoints.add(secondPoint);
					firstPoint = secondPoint;
				}

				secondPoint = thirdPoint;
			}

			// Last point is always on hull. 
			newGeoPoints.add(secondPoint);

			//System.out.println("CalculateHull: count = " + newGeoPoints.size());
			//for (GeoPoint geoPoint : newGeoPoints)
			//	System.out.println("    point: " + geoPoint);

			return newGeoPoints;
		}
	}

	public HullResult calculateConvexHull(JavaRDD<GeoPoint> geoPoints)
	{
		List<GeoPoint> outputPoints;

		// Upper hull.
		JavaRDD<GeoPoint> sortedGeoPoints = geoPoints.sortBy(new SortPoints(), true, 1);

		//outputPoints = sortedGeoPoints.collect();
		//for (GeoPoint geopoint : outputPoints) {
		//  System.out.println("upperSortedGeoPoints: " + geopoint.toString());
		//}

		JavaRDD<GeoPoint> upperGeoPoints = sortedGeoPoints;
		JavaRDD<GeoPoint> newGeoPoints;

		do {
			newGeoPoints = upperGeoPoints;
			upperGeoPoints = newGeoPoints.mapPartitions(new CalculateHull());

			//System.out.printf("Strip Iteration: newCnt = %d, newerCnt= %d%n",newGeoPoints.count(), upperGeoPoints.count());
		} while (newGeoPoints.count() != upperGeoPoints.count());

		// Coalesce the upper hull so we can repeat the algorithm on the combined set.
		//JavaRDD<GeoPoint> coalescedGeoPoints = upperGeoPoints.coalesce(1,true);

		//do {
		//	newGeoPoints = coalescedGeoPoints;
		//	coalescedGeoPoints = newGeoPoints.mapPartitions(new CalculateHull());

		//System.out.printf("Strip Iteration: newCnt = %d, newerCnt= %d%n",newGeoPoints.count(), upperGeoPoints.count());
		//} while (newGeoPoints.count() != coalescedGeoPoints.count());
		//upperGeoPoints = coalescedGeoPoints;


		//outputPoints = upperGeoPoints.collect();
		//for (GeoPoint geopointslope : outputPoints) {
		//    System.out.println("upperHullGeoSlopePoints: " + geopointslope.toString());
		//}

		// lower hull.
		JavaRDD<GeoPoint> lowerSortedGeoPoints = geoPoints.sortBy(new SortPoints(), false, 1);

		//outputPoints = lowerSortedGeoPoints.collect();
		//for (GeoPoint geopoint : outputPoints) {
		//    System.out.println("lowerSortedGeoPoints: " + geopoint.toString());
		//}

		JavaRDD<GeoPoint> lowerGeoPoints = lowerSortedGeoPoints;
		do {
			newGeoPoints = lowerGeoPoints;
			lowerGeoPoints = newGeoPoints.mapPartitions(new CalculateHull());

			//System.out.printf("Strip Iteration: newCnt = %d, newerCnt= %d%n",newGeoPoints.count(), lowerGeoPoints.count());
		} while (newGeoPoints.count() != lowerGeoPoints.count());

		// Coalesce the upper hull so we can repeat the algorithm on the combined set.
		//coalescedGeoPoints = lowerGeoPoints.coalesce(1,true);

		//do {
		//	newGeoPoints = coalescedGeoPoints;
		//	coalescedGeoPoints = newGeoPoints.mapPartitions(new CalculateHull());

		//System.out.printf("Strip Iteration: newCnt = %d, newerCnt= %d%n",newGeoPoints.count(), upperGeoPoints.count());
		//} while (newGeoPoints.count() != coalescedGeoPoints.count());
		//lowerGeoPoints = coalescedGeoPoints;

		//outputPoints = lowerGeoPoints.collect();
		//for (GeoPoint geopointslope : outputPoints) {
		//    System.out.println("lowerHullGeoSlopePoints: " + geopointslope.toString());
		//}

		return new HullResult(upperGeoPoints, lowerGeoPoints);
	}


	public void operation(String inputFilePath, String outputFilePath)
	{
		SparkConf sc = new SparkConf().setAppName("GeometryConvexHull");
		JavaSparkContext jsc = new JavaSparkContext(sc);

		JavaRDD<String> inputStrings = jsc.textFile(inputFilePath);

		//List<String> outputStrings = inputStrings.collect();
		//for (String outString : outputStrings) {
		//    System.out.println("convexHullInputStrings: " + outString);
		//}

		JavaRDD<GeoPoint> geoPoints = inputStrings.map(new ParsePoints()); 
		HullResult convexHullResult = calculateConvexHull(geoPoints);

		// Return the union of the upper and lower hulls, with the repeated elements removed.
		JavaRDD<GeoPoint> resultGeoPoints = convexHullResult.upperHull().union(convexHullResult.lowerHull()).distinct(1).sortBy(new SortPoints(), true, 1);

		GeoSpatialUtils.deleteHDFSFile(outputFilePath);

		List<GeoPoint> outputPoints = resultGeoPoints.collect();
		for (GeoPoint geopoint : outputPoints) {
			System.out.println("convexHullResultPoints: " + geopoint.toString());
		}

		resultGeoPoints.saveAsTextFile(outputFilePath);  

		jsc.close();
	}

	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation
	 * @param outputLocation
	 * 
	 */

	public static void main(String[] args) {

		//Initialize, need to remove existing in output file location.

		//Implement 

		//Output your result, you need to sort your result!!!
		//And,Don't add a additional clean up step delete the new generated file...

		new convexHull().operation(args[0], args[1]);
	}
}
