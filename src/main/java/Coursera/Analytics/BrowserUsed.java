package Coursera.Analytics;

/*
 * Author : Niharika
 * Program to calculate most used browser for Coursera PSY-001 course
 * 
 */
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class BrowserUsed {
	public static void main(String[] args) {


		HelperClass h = new HelperClass();
		String path = h.getpath();
		String inputfile = path+"PSY-Data.txt";
		String outputfile = path+"PSY_Output_Browser";
	SparkConf conf = new SparkConf();
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	JavaRDD<String> clickevents = sc.textFile(inputfile);
	
	try{
	
		// Form tuples of every user and their user_agents used
	JavaPairRDD<String , String> user_browser = clickevents.mapToPair(new PairFunction<String , String , String>() {
		

		public Tuple2<String , String>  call(String s)
		{
			String features[] = s.split("\t");
			Tuple2<String , String> t = new Tuple2<String, String>(features[2] , features[10]);
			return t;
		}
	});
	
	//Take note of distinct user and user_agent RDDS.
	JavaPairRDD<String , String> dist_usr = user_browser.distinct();
	
	//Change the user_agent information to get the Browser details
	JavaPairRDD<String , Integer> browser_detected = dist_usr.mapToPair(new PairFunction<Tuple2<String,String>, String, Integer>() {

		public Tuple2<String, Integer> call(Tuple2<String, String> t)
				throws Exception {
			String s = t._2();
			return browserdetection(s);
		}

		private Tuple2<String, Integer> browserdetection(String s) {
			if(s.contains("AppleWebKit/"))
			return new Tuple2<String , Integer>("WebKit" , 1);
			else if(s.contains("Opera/"))
				return new Tuple2<String , Integer>("Presto" , 1);
			else if(s.contains("Trident/"))
				return new Tuple2<String , Integer>("Trident" , 1);
			else if(s.contains("Gecko/"))
				return new Tuple2<String , Integer>("Gecko" , 1);
			else if(s.contains("Chrome/"))
				return new Tuple2<String , Integer>("Blink" , 1);
			return new Tuple2<String , Integer>("None" , 1);
			
		}
		
		
	});
	
	//Sum the count values for every browser
	JavaPairRDD<String , Integer> browser_count = browser_detected.reduceByKey(new addvals());
	
	//Swap the key value positions to sort the RDD based on the count
	JavaPairRDD< Integer , String> swapped_browser = browser_count.mapToPair(new swappedvals());
	
	//Sort the RDD and pick the top most Tuple out of it
	Tuple2<Integer, String> sorted_browser = swapped_browser.sortByKey(new Sortvals(),false).first();
	
	List<String>browser = Arrays.asList(sorted_browser._2());
	
	JavaRDD<String> most_usedbrowser = sc.parallelize(browser);
	
	
	
	most_usedbrowser.saveAsTextFile(outputfile);

}
	catch(Exception e)
	{
		
	}
	}
}
