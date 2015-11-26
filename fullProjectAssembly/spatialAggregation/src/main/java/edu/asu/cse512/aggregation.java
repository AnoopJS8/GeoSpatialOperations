package edu.asu.cse512;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class aggregation implements Serializable {
	private static final long serialVersionUID = -78390015906582065L;

	public void spatialAggregationMain(String inpFile,String outFile) {

		SparkConf conf = new SparkConf().setAppName("Aggregation");

		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> result = context.textFile(inpFile).map(parseLine);

		result.coalesce(1).saveAsTextFile(outFile);
		context.close();
	}
	
	public final static Function<String,String> parseLine=new Function<String, String >(){
		private static final long serialVersionUID = 7512883574582331862L;
		
		public String call(String s) {
			String[] inputLine = s.split(",");
			return inputLine[0]+","+(inputLine.length-1);
		}		
	};
	
	public static void main(String args[]){
		String joinPutFile="joinOut"+args[2];
		// Initialize, need to remove existing in output file location.
		GeoSpatialUtils.deleteHDFSFile(joinPutFile);
		GeoSpatialUtils.deleteHDFSFile(args[2]);
		Join join = new Join();
		join.spatialJoinMain(args[0], args[1], joinPutFile , args[3]);
		new aggregation().spatialAggregationMain(joinPutFile,args[2]);
		GeoSpatialUtils.deleteHDFSFile(joinPutFile);
	}
}
