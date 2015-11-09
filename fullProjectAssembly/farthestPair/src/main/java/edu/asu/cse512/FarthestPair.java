package edu.asu.cse512;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import edu.asu.cse512.FarthestPair;
import edu.asu.cse512.GeoPoint;
import edu.asu.cse512.GeoPointPair;
import edu.asu.cse512.GeoSpatialUtils;
import edu.asu.cse512.convexHull;
import edu.asu.cse512.HullResult;
import scala.Tuple2;

public class FarthestPair implements Serializable
{	
	private static final long serialVersionUID = 196768568656874181L;
	private static final Logger logger = Logger.getLogger(FarthestPair.class);
	class ParsePoints implements Function<String, GeoPoint>, Serializable
	{
		private static final long serialVersionUID = -3885195256936448019L;

		@Override
		public GeoPoint call(String inputLine)
		{
		    return new GeoPoint(inputLine);
		}
	}
	class SortPoints implements Function<GeoPoint,GeoPoint>, Serializable
	{
		private static final long serialVersionUID = 8225302338329281442L;

		public GeoPoint call(GeoPoint geoPoint) throws Exception {
			return geoPoint;
		}
	}
	public boolean operation(String sparkMasterIP, String inputFileName, String outputFileName){
		boolean success=false;
		SparkConf sc = new SparkConf().setAppName("group20.operations")
				.setMaster(sparkMasterIP);
		JavaSparkContext context = new JavaSparkContext(sc);
		JavaRDD<String> inputFileRDD = context.textFile(inputFileName);
		if(logger.isDebugEnabled()){
			logger.debug("inputFile RDD: "+inputFileRDD.count());
		}
		JavaRDD<GeoPoint> geoPoints = inputFileRDD.map(new ParsePoints()); 
		convexHull gch=new convexHull();
		HullResult convexHullResult = gch.calculateConvexHull(geoPoints);

		// Return the union of the upper and lower hulls, with the repeated elements removed.
		JavaRDD<GeoPoint> resultGeoPoints = convexHullResult.upperHull().union(convexHullResult.lowerHull()).distinct(1);
		if(resultGeoPoints!=null){
			GeoPointPair farthestPairs = resultGeoPoints
					.cartesian(resultGeoPoints)
					.map(new Function<Tuple2<GeoPoint, GeoPoint>, GeoPointPair>() {		
						private static final long serialVersionUID = 1L;
						public GeoPointPair call(Tuple2<GeoPoint, GeoPoint> geoPointPair) {
							return new GeoPointPair(geoPointPair._1(), geoPointPair._2());}})
					.reduce(new Function2<GeoPointPair, GeoPointPair, GeoPointPair>() {
						private static final long serialVersionUID = 1L;
						public GeoPointPair call(GeoPointPair p, GeoPointPair q) {
							return p.getDistance() > q.getDistance() ? p : q;
						}});
			GeoSpatialUtils.deleteHDFSFile(outputFileName);		    
			resultGeoPoints.saveAsTextFile(outputFileName);
		}
		context.close();
		return success;
	}
	
	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation
	 * @param outputLocation
	 * 
	*/
    public static void main( String[] args )
    {
        //Initialize, need to remove existing in output file location.
    	
    	//Implement 
    	
    	//Output your result, you need to sort your result!!!
    	//And,Don't add a additional clean up step delete the new generated file...
    }
}
