package Coursera.Analytics;
/*
 * Author : Niharika
 * Program to calculate most preferred language for Coursera PSY-001 course
 * 
 */
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class PreferredLanguage {
	
	public static void main(String[] args)
	{

		HelperClass h = new HelperClass();
		String path = h.getpath();
		String inputfile = path+"PSY-Data.txt";
		String outputfile = path+"PSY_Output_Language";
		
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> clickevents = sc.textFile(inputfile);
		
		try{
		//Form  pairs of users and their languages directly from the file. 
		JavaPairRDD<String , String> user_lang = clickevents.mapToPair(new PairFunction<String , String , String>() {
			

			public Tuple2<String , String>  call(String s)
			{
				String features[] = s.split("\t");
				Tuple2<String , String> t = new Tuple2<String, String>(features[2] , features[7]);
				return t;
			}
		});
		
		//To get distinct pairs (Tuples) of users and their languages string
		JavaPairRDD<String , String> dist_usr = user_lang.distinct();
		
		//map each language of user in each string to an ArrayList of Langq class having each lang and its users preference.
		JavaRDD<ArrayList<Langq>> l = dist_usr.map(new Function<Tuple2<String , String> , ArrayList<Langq>>()
		{

			public ArrayList<Langq> call(Tuple2<String, String> v1)
					throws Exception {
				
				return languageprferences(v1._2());
			}
			
			//function to take note of each language and its preference value.
			private ArrayList<Langq> languageprferences(String s) {
				ArrayList<Langq> a = new ArrayList<Langq>();
				String[] arr = s.split(";");
				for(int i=0;i<arr.length;i++)
				{
					if(arr[i].substring(0,1) == "q")
					{
						double d = Double.parseDouble(arr[i].substring(2,3));
						a.get(i-1).setq(d); 
						
						String[] l = arr[i].substring(3).split(",");
						for(int j=0 ; j<l.length ; j++)
						{
							String lang = l[j].split("-")[0];
							Langq newlang = new Langq(lang , 1.0D);
							a.add(newlang);
						}
					}
					else
					{
						String[] l = arr[i].substring(3).split(",");
						for(int j=0 ; j<l.length ; j++)
						{
							String lang = l[j].split("-")[0];
							Langq newlang = new Langq(lang , 1.0D);
							a.add(newlang);
						}	
					}
				}
				return a;
			}
			
		});
		// To combine all the values in ArrayList to JavaRDD
		JavaPairRDD<String , Double> lang_vals =  l.flatMapToPair(new PairFlatMapFunction<ArrayList<Langq>, String, Double>() {

			public Iterable<Tuple2<String, Double>> call(ArrayList<Langq> t)
					throws Exception {
				 List <Tuple2<String , Double>> iter = new ArrayList<Tuple2<String , Double>>();
				 for(Langq q: t)
				 {
					 String lan = q.getlang();
					 double v = q.getpref(); 
					 iter.add(new Tuple2<String , Double> (lan , v));
				 }
				return iter;
			}
		});
		
					
		//sum of the languages preference values.
		JavaPairRDD<String , Double> lang_count = lang_vals.reduceByKey(new Function2<Double, Double, Double>() {
			
			public Double call(Double v1, Double v2) throws Exception {
				
				return v1+v2;
			}
		});
		
		//Swap the positions of the key value pairs in the tuples
		JavaPairRDD<Double , String> swapped_lancount = lang_count.mapToPair(new PairFunction<Tuple2<String , Double> , Double , String>()
				{
			public Tuple2<Double , String> call(Tuple2<String , Double> t)
			{
				return new Tuple2<Double , String>(t._2(), t._1());
			}
				});
		
		//Sort the RDD using the language counts and obtain the top language
		Tuple2<Double , String> sorted_langcount = swapped_lancount.sortByKey(new SortvalsDouble(),false).first();
		
		
		
		List<String>language = Arrays.asList(sorted_langcount._2());
		
		JavaRDD<String> most_usedlang = sc.parallelize(language);
		
		
		
		most_usedlang.saveAsTextFile(outputfile);
		
		}
		
		catch(Exception e)
		{
			
		}
	}

}
