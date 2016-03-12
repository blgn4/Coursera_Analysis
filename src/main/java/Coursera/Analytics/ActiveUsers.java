package Coursera.Analytics;

/*
 * Author : Niharika
 * Program to calculate Active Users on Coursera PSY-001 course
 * 
 */
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;



public class ActiveUsers {
	
	public static void main(String[] args)
	{

		HelperClass h = new HelperClass();
		String path = h.getpath();
		String inputfile = path+"PSY-Data.txt";
		String outputfile = path+"PSY_Output_ActiveUser";
		SparkConf sparkconf = new SparkConf().setAppName("Coursera_Analytics");
		JavaSparkContext sc = new JavaSparkContext(sparkconf);
		final Accumulator<Integer> count = sc.accumulator(0);
		final Accumulator<Integer> check = sc.accumulator(0);
		
		try
		{
			//Load the file contents to RDD
			JavaRDD<String> clickdetails = sc.textFile(inputfile);
			
			//Form a tuple of user as key and 1 as value
		JavaPairRDD<String , Integer> activeusers = clickdetails.mapToPair( new PairFunction<String , String , Integer>()
				{	
				public Tuple2<String,Integer> call(String s)
				{
			            String[] features = s.split("\t");
			            return new Tuple2<String, Integer>(features[2] , 1);
				}
				});
		
		//Add the values of all users to get only user counts
		JavaPairRDD<String , Integer> user_count = activeusers.reduceByKey(new addvals());
		
		//Swap the tuple to get count as key and username as value
		JavaPairRDD<Integer , String> swappedvalues = user_count.mapToPair(new swappedvals());
		
		//Sort the result to take note of the top few users
		List<Tuple2<Integer, String>> sorted_user = swappedvalues.sortByKey(new Sortvals() , false).collect();
		
		//To calculate a way to get upto top 10 best active users.
		ArrayList<String> array_topusers = new ArrayList<String>();
		int n =0;
		int size =0;
		int prv=-1;
		int ch =0;
		for(Tuple2<Integer, String> t : sorted_user)
		{	if(size < 10)
		{
			if(n== 1)
			{
				array_topusers.add(t._2()+"-->"+t._1().toString());
			}
			else
			{
				if(prv != t._1() )
				{
					size++;					
				}
				
				if(size < 10 )
				array_topusers.add(t._2()+"-->"+t._1().toString());
		    }			
		}
		
			prv = t._1();
	         n++;	
		}
		String[] arr = new String[n];
		for(int i =0;i<n ;i++)
		{
			arr[i] = array_topusers.get(i);
		}
		List<String> usernames = Arrays.asList(arr);
		
		JavaRDD<String> top_user = sc.parallelize(usernames);
		System.out.println(top_user.first());
		top_user.saveAsTextFile(outputfile);	
		
		}
		catch (Exception e)
		{
			
		}
	}

}
