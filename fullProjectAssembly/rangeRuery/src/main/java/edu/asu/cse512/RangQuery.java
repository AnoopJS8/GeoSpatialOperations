package edu.asu.cse512;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;


public class RangQuery implements Serializable {

	private static final long serialVersionUID = -4899212954483667421L;
	private static final Logger logger = Logger.getLogger(RangQuery.class);

	public void rangeQuery(String input1, String input2, String output) {
		SparkConf sc = new SparkConf().setAppName("RangQuery");
		JavaSparkContext context = new JavaSparkContext(sc);
		JavaRDD<String> file1 = context.textFile(input1);
		JavaRDD<String> file2 = context.textFile(input2);

		JavaRDD<GeoPoint> points = file1.map(new Function<String, GeoPoint>() {

			private static final long serialVersionUID = 4103513079613043110L;

			public GeoPoint call(String str) throws Exception {
				// TODO Auto-generated method stub
				String[] input_temp = str.split(",");

				if (input_temp.length > 2) {
					int id = Integer.parseInt(input_temp[0]);
					double x1 = Double.parseDouble(input_temp[1]);
					double y1 = Double.parseDouble(input_temp[2]);

					GeoPoint geopoint = new GeoPoint(id, x1, y1);
					return geopoint;
				}
				return null;
			}

		});
		
		
		JavaRDD<Rectangle> queryWindow = file2.map(new Function<String, Rectangle>() {

			private static final long serialVersionUID = -3281992130910139220L;

			public Rectangle call(String s) throws Exception {

				String[] input_temp = s.split(",");
				double x1, y1, x2, y2;
				if (input_temp.length > 3) {
					x1 = Double.parseDouble(input_temp[0]);
					y1 = Double.parseDouble(input_temp[1]);
					x2 = Double.parseDouble(input_temp[2]);
					y2 = Double.parseDouble(input_temp[3]);
					Rectangle rectangle = new Rectangle(x1, y1, x2, y2);
					return rectangle;
				}

				return null;
			}
		});
		
		
		final Broadcast<Rectangle> cachedWindow = context.broadcast(queryWindow.first());


		JavaRDD<GeoPoint> result = points.filter(new Function<GeoPoint, Boolean>() {
			private static final long serialVersionUID = -6891257309320068134L;

			public Boolean call(GeoPoint p) {
				//if (cachedWindow != null && cachedWindow.value() != null) {
					//return true;
			    //return cachedWindow.value().contains(p.getX(), p.getY()); 
				return cachedWindow.value().containsPoints(p);
				//} else {
				//	return false;
				//}
			}

		});

		logger.debug(">>>> result: " + result.count());
		GeoSpatialUtils.deleteHDFSFile(output);
		result.coalesce(1).saveAsTextFile(output);
		context.close();
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
