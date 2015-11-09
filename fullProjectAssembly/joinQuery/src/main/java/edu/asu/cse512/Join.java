package edu.asu.cse512;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import edu.asu.cse512.GeoPoint;
import edu.asu.cse512.GeoSpatialUtils;
import edu.asu.cse512.Rectangle;

/**
 * Hello world!
 *
 */
public class Join  implements Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1150784842364424486L;

	public void spatialJoinMain(String inputFile1, String inputFile2, String outputFile, String inputType, String sparkMasterIP){

		SparkConf conf = new SparkConf().setAppName("group20.operations")
				.setMaster(sparkMasterIP);

		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<Rectangle> queryRectangle = context.textFile(inputFile2).map(createRectangle);

		if(inputType.equalsIgnoreCase("point")){
			JavaRDD<GeoPoint> input1points = context.textFile(inputFile1).map(createPoint);
			final Broadcast<List<GeoPoint>> broadCastPoints = context.broadcast(input1points.toArray());
			functionForPoint(queryRectangle, broadCastPoints, outputFile);
		} else if(inputType.equalsIgnoreCase("rectangle")){
			JavaRDD<Rectangle> input1Rectangles = context.textFile(inputFile1).map(createRectangle);
			final Broadcast<List<Rectangle>> broadCastRectangles = context.broadcast(input1Rectangles.toArray());	
			functionForRectangle(queryRectangle, broadCastRectangles, outputFile);
		}
		context.close();
	}

	public void functionForPoint(JavaRDD<Rectangle> queryRectangle, final Broadcast<List<GeoPoint>> broadCastPoints , String outputFile){

		JavaRDD<String> result = queryRectangle
				.map(new Function<Rectangle, String>() {
					private static final long serialVersionUID = -3539962860685816403L;

					public String call(Rectangle input2Rectangle) throws Exception {

						String temp = String.valueOf(input2Rectangle.getId());
						List<Integer> count = new ArrayList<>();
						List<GeoPoint> input1Points = (ArrayList<GeoPoint>) broadCastPoints.getValue();
						for (int i = 0; i < input1Points.size(); i++) {
							if (input1Points.get(i).containsPoint(input2Rectangle)){
								count.add(input1Points.get(i).getId());
							}
						}
						Collections.sort(count);
						Iterator<Integer> iterator = count.iterator();
						while (iterator!=null && iterator.hasNext()) {
							temp = temp + "," + String.valueOf(iterator.next());
						}
						if(iterator==null){
							temp = temp + ", NULL";
						}
						return temp;
					}
				});
		GeoSpatialUtils.deleteHDFSFile(outputFile);		    
		result.coalesce(1).saveAsTextFile(outputFile);
	}

	public void functionForRectangle(JavaRDD<Rectangle> queryRectangle, final Broadcast<List<Rectangle>> broadCastRectangles, String outputFile){

		JavaRDD<String> result = queryRectangle
				.map(new Function<Rectangle, String>() {
					private static final long serialVersionUID = -7174473040113819910L;
					public String call(Rectangle queryRectangle) throws Exception {
						String temp = String.valueOf(queryRectangle.getId());
						List<Integer> count = new ArrayList<Integer>();
						List<Rectangle> inputRectangles = (ArrayList<Rectangle>) broadCastRectangles.getValue();
						for (Rectangle inputRectangle : inputRectangles) {		
							if (queryRectangle.containsRectangle(inputRectangle)){
								count.add(inputRectangle.getId());
							}
						}
						Collections.sort(count);
						Iterator<Integer> iterator = count.iterator();
						while (iterator!=null && iterator.hasNext()) {
							temp = temp + "," + String.valueOf(iterator.next());
						}
						if(iterator==null){
							temp = temp + ", NULL";
						}
						return temp;
					}
				});
		GeoSpatialUtils.deleteHDFSFile(outputFile);		    
		result.coalesce(1).saveAsTextFile(outputFile);
	}	 

	public final static Function<String, Rectangle> createRectangle = new Function<String, Rectangle>() {

		private static final long serialVersionUID = 1421620964663593553L;

		public Rectangle call(String s) {
			String[] rectangleArray = s.split(",");
			double x1, x2, y1, y2;
			int id;
			id = Integer.parseInt(rectangleArray[0]);
			x1 = Double.parseDouble(rectangleArray[1]);
			y1 = Double.parseDouble(rectangleArray[2]);
			x2 = Double.parseDouble(rectangleArray[3]);
			y2 = Double.parseDouble(rectangleArray[4]);
			return new Rectangle(id, x1, y1, x2, y2);
		}
	};

	public final static Function<String, GeoPoint> createPoint = new Function<String, GeoPoint>() {

		private static final long serialVersionUID = -1461135805987944548L;

		public GeoPoint call(String s) {
			String[] rectangleArray = s.split(",");
			double x1, y1;
			int id;
			id = Integer.parseInt(rectangleArray[0]);
			x1 = Double.parseDouble(rectangleArray[1]);
			y1 = Double.parseDouble(rectangleArray[2]);
			GeoPoint point = new GeoPoint(id, x1, y1);
			return point;
		}
	};

	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation 1
	 * @param inputLocation 2
	 * @param outputLocation
	 * @param inputType 
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
