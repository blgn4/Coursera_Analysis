package Coursera.Analytics;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;



public class SwethaProg {
	
	 private class Records implements Serializable {
	        String key;
	        String value;
	        String username;
	        long timestamp;
	        String page_url;
	        String client;
	        String session;
	        String language;
	        String from;
	        String user_ip;
	        String user_agent;
	    }

	    private static class CustomComparator implements Serializable, Comparator { 
	        public int compare(Object o11, Object o12) {
	            Tuple2<Integer, String> o1 = (Tuple2<Integer, String>) o11;
	            Tuple2<Integer, String> o2 = (Tuple2<Integer, String>) o12;
	            return Integer.compare(o1._1, o2._1);
	        }
	    }

	    public static void getMaxCountUsers(JavaRDD<String> rdd_records, int n, JavaSparkContext sc, String opfilePath) throws IOException {
	        FileWriter fw = new FileWriter(opfilePath+"TopActiveUsers.txt");
	        JavaPairRDD<String, Integer> pairs = rdd_records.mapToPair(new PairFunction<String, String, Integer>() {
	            public Tuple2<String, Integer> call(String s) {
	            	String[] features = s.split("\t");
	                return new Tuple2<String, Integer>(features[2], 1);
	            }
	        });
	        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
	            public Integer call(Integer a, Integer b) { 
	                return a + b; 
	            }
	        });
	        JavaPairRDD<Integer, String> swapKeyValues = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
	            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) {
	                return item.swap();
	            }
	        });
	        JavaPairRDD<Integer, String> topUsersRDD = swapKeyValues.sortByKey(false);
	        List<Tuple2<Integer, String>> topUsers = topUsersRDD.top(n, new CustomComparator());
	        for(Tuple2<Integer, String> user:topUsers) {
	            System.out.println("\nUserName : "+user._2+"\tCount : "+user._1);
	            fw.write("\nUserName : "+user._2+"\tCount : "+user._1);
	        }
	        fw.close();
	        JavaRDD<Tuple2<Integer, String>> output= sc.parallelize(topUsers);
	        output.saveAsTextFile(opfilePath+"/TopActiveUsers");
	    }


	    public static void getMaxCountVideos(JavaRDD<String> rdd_records, int n ,JavaSparkContext sc, String opfilePath) throws IOException
	    {
	        FileWriter fw = new FileWriter(opfilePath+"TopViewedVideos.txt");
	        JavaRDD<String> rdd_records_filtered = rdd_records.filter(new Function<String, Boolean>() {
	            public Boolean call(String r) throws Exception {
	            	String[] features = r.split("\t");
	                if(features[0].contains("video")){
	                    return true;
	                }
	                return false;
	            }
	        });
	        JavaPairRDD<String, String> tuples = rdd_records.mapToPair(new PairFunction<String, String, String>() {
	            public Tuple2<String, String> call(String s) {
	            	String[] features = s.split("\t");
	                return new Tuple2<String, String>(features[4], features[2]);
	            }
	        }).distinct();
	        JavaPairRDD<String, Integer> pairs = tuples.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
	            public Tuple2<String, Integer> call(Tuple2<String, String> s) {
	                return new Tuple2<String, Integer>(s._1, 1);
	            }
	        });
	        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
	            public Integer call(Integer a, Integer b)
	            { return a + b; }
	        });
	        JavaPairRDD<Integer, String>swapKeyValues = counts.mapToPair(
	                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
	                    public Tuple2<Integer, String> call(Tuple2<String, Integer> item) {
	                        return item.swap();}});
	        JavaPairRDD<Integer, String> topVideosRDD = swapKeyValues.sortByKey(false);
	        List<Tuple2<Integer, String>> topVideos = topVideosRDD.top(n, new CustomComparator());
	        for(Tuple2<Integer, String> video:topVideos){
	            System.out.println("\nVideo Link : "+video._2+"\tCount : "+video._1);
	            fw.write("\nVideo Link : "+video._2+"\tCount : "+video._1);
	        }
	        JavaRDD<Tuple2<Integer, String>> output= sc.parallelize(topVideos);
	        output.saveAsTextFile(opfilePath+"/TopViewedVideos");
	        fw.close();
	    }

	    public static void main(String args[]) {
	        SparkConf sparkConf = new SparkConf().setAppName("testapp");
	        JavaSparkContext sc = new JavaSparkContext(sparkConf);
	        Scanner scn = new Scanner(System.in);
	        HelperClass h = new HelperClass();
			String path = h.getpath();
			String inputfile = path+"PSY-Data.txt";
	        System.out.println("\nEnter the input file : ");
	        String ipfilePath = inputfile;
	        System.out.println("\nEnter the output file path : ");
	        String opfilePath ="/home/worker2/Documents"; //scn.nextLine();
	        System.out.println("\nEnter the number of top active users to be returned (0 if no preference) : ");
	        int userNo = 0;//scn.nextInt();
	        System.out.println("\nEnter the number of top viewed videos to be returned (0 if no preference) : ");
	        int videoNo = 0;//scn.nextInt();
	        if(userNo==0)
	            userNo = 10;
	        if(videoNo==0)
	            videoNo = 10;
	        try {
	            JavaRDD<String> data = sc.textFile(ipfilePath);
	           
	            System.out.println("\nList of most active users: ");
	            getMaxCountUsers(data, userNo, sc, opfilePath);

	            System.out.println("\nMost viewed videos: ");
	            getMaxCountVideos(data, videoNo, sc, opfilePath);
	        }
	        catch(Exception ex)
	        {
	            ex.printStackTrace();
	        }
	        System.out.println("done!");
	        sc.close();
	    }

}
