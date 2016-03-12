package Coursera.Analytics;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class HelperClass {

	public String inputfile ;
	public String path ;
	
	public HelperClass()
	{
		inputfile = "psy-001_clickstream_export_snippet.txt";
	    path = "/home/worker2/Documents/";
	}
	public String getinputFileName()
	{
		return inputfile;
	}
	
	public String getpath()
	{
		return path;
	}
}

class addvals implements Function2<Integer, Integer, Integer>{ 
	
	public Integer call(Integer v1, Integer v2) throws Exception {
		
		return v1+v2;
	}
};

class Sortvals implements Serializable , Comparator<Integer>
{
	

	public int compare(Integer i1, Integer i2) {
		if(i1 > i2)
			return 1;
		else if(i1 < i2)
			return -1;
		else
		return 0;
	}
};

class swappedvals implements PairFunction<Tuple2<String,Integer>, Integer, String> {
	
	public Tuple2<Integer,String> call(Tuple2<String , Integer> tuple)
	{
		
		String is = tuple._1();
		int ii = tuple._2();
		return new Tuple2(ii , is);
		
	}
};
class SortvalsDouble implements Serializable , Comparator<Double>
{
	

	public int compare(Double i1, Double i2) {
		if(i1 > i2)
			return 1;
		else if(i1 < i2)
			return -1;
		else
		return 0;
	}
};

class Langq{
	String lan;
	double q;
	public Langq(String s , double d)
	{
	  lan = s;
	  q =d;
	}
	public void setq(double d)
	{
		q=d;
	}
	public String getlang()
	{
		return lan;
	}
	public double getpref()
	{
		return q;
	}
};
