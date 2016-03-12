package Coursera.Analytics;
/*
 * Author : Niharika
 * Program to calculate Top videos viewed on Coursera PSY-001 course
 * 
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TopVideos {
	
	public static void main(String[] args)
	{
		HelperClass h = new HelperClass();
		String path = h.getpath();
		String inputfile = path+"PSY-Data.txt";
		String outputfile = path+"PSY_Output_TopVideos";
		
		SparkConf sparkconf = new SparkConf().setAppName("Coursera_Analytics2");
		JavaSparkContext sc = new JavaSparkContext(sparkconf);

		final Accumulator<Integer> count = sc.accumulator(0);
		final Accumulator<Integer> check = sc.accumulator(0);
		JavaRDD<String> clickdetails = sc.textFile(inputfile);
		try
		{
			//Formed a Tuple with key as user and action_type and with a value 1
			JavaPairRDD<String,Tuple2<Tuple2<String,String> , Integer>> activeusers = clickdetails.mapToPair( new PairFunction<String , String , Tuple2<Tuple2<String ,String>, Integer>>()
					{	
					public Tuple2<String,Tuple2<Tuple2<String,String>, Integer>> call(String s)
					{
				            String[] features = s.split("\t");
				            Tuple2<String , String> vid_user = new Tuple2(features[2],features[4]);
				            Tuple2<Tuple2<String , String> , Integer> vid_user_count = new Tuple2(vid_user , 1);
				            return new Tuple2(features[0] , vid_user_count);
					}
					}
					);
			//Filter out  Tuples of user and their videos watched keeping the value as 1.
			JavaPairRDD<String,Tuple2<Tuple2<String,String> , Integer>> vidwatchers = activeusers.filter(new Function<Tuple2<String,Tuple2<Tuple2<String,String>,Integer>>, Boolean>() {
				
				public Boolean call(
						Tuple2<String, Tuple2<Tuple2<String, String>, Integer>> v1)
						throws Exception {
					return v1._1().equals("user.video.lecture.action");
				}
			});
			//Reducing the tuple to user and videourl as key and 1 as value
			JavaPairRDD<Tuple2<String,String> , Integer> usrvid = vidwatchers.mapToPair( new PairFunction<Tuple2<String,Tuple2<Tuple2<String,String>,Integer>> , Tuple2<String , String> , Integer>()
					{	
					public Tuple2<Tuple2<String, String>, Integer> call(
							Tuple2<String, Tuple2<Tuple2<String, String>, Integer>> t)
							throws Exception {
						Tuple2<String , String> vid_user = t._2()._1();
						return new Tuple2(vid_user , 1);
					}
					}
					);
			
			//Adding up the values of each Tuple to get distinct user- videos
			JavaPairRDD<Tuple2<String,String> , Integer> uservid_count = usrvid.reduceByKey(new addvals());
			
			//Forming a Tuple with video as key and 1 as value
			JavaPairRDD<String , Integer> videos = uservid_count.mapToPair( new PairFunction< Tuple2<Tuple2<String,String> , Integer> , String , Integer>()
					{	
					
					public Tuple2<String, Integer> call(
							Tuple2<Tuple2<String, String>, Integer> t)
							throws Exception {
						
						return new Tuple2(t._1()._2(), 1);
					}

					
					}
					);
			//Taking total counts of each video.
			JavaPairRDD<String , Integer> vid_count = videos.reduceByKey(new addvals());
			
			//Swap the Tuple with key as count and value as video
			JavaPairRDD<Integer , String> swappedcounts = vid_count.mapToPair(new swappedvals());
			//Sort the RDD in descending order to collect the top few famously watched videos.
			List<Tuple2<Integer, String>> sorted_video = swappedcounts.sortByKey(new Sortvals() , false).collect();
			ArrayList<String> array_topvideos = new ArrayList<String>();
			int n =0;
			int size =0;
			int prv=-1;
			int ch =0;
			for(Tuple2<Integer, String> t : sorted_video)
			{	if(size < 10)
			{
				if(n== 1)
				{
					array_topvideos.add(t._2()+"-->"+t._1().toString());
				}
				else
				{
					if(prv != t._1() )
					{
						size++;
						
					}
					
					if(size < 10)
						array_topvideos.add(t._2()+"-->"+t._1().toString());
			    }			
			}
			
				prv = t._1();
		         n++;	
			}
			String[] arr = new String[n];
			for(int i =0;i<n ;i++)
			{
				arr[i] = array_topvideos.get(i);
			}
			List<String> vidnames = Arrays.asList(arr);
			
			JavaRDD<String> top_vids = sc.parallelize(vidnames);
			System.out.println(top_vids.first());
			
			top_vids.saveAsTextFile(outputfile);	
			
		
		}
		catch(Exception e)
		{
			
		}
	}

}
