package Coursera.Analytics;

/*
 * Author : Niharika
 * Program to convert click-stream data for Coursera PSY-001 course from JSON to CSV format
 * 
 */ 

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonToCSV {
	
	public static void main(String[] args) throws IOException, ParseException
	{

		HelperClass h = new HelperClass();
		String path = h.getpath();
		String inputfilename = h.getinputFileName();
		String inputfile = path+inputfilename;
		String outputfile = path+"PSY-Data.txt";
		File f = new File(inputfilename);
		BufferedReader br = new BufferedReader(new FileReader(f));
		File fout = new File(outputfile);
		FileWriter fw = new FileWriter(fout);
		String line;
		while((line=br.readLine())!= null)
		{
			JSONObject ob = (JSONObject) new JSONParser().parse(line);
			String key = (String) ob.get("key");
			String value = (String) ob.get("value");
			String username = (String) ob.get("username");
			String timestamp = (String) ob.get("timestamp");
			String page_url = (String) ob.get("page_url");
			String client = (String) ob.get("client");
			String session = (String) ob.get("session");
			String language = (String) ob.get("language");
			String from = (String) ob.get("from");
			String user_ip = (String) ob.get("user_ip");
			String user_agent = (String) ob.get("user_agent");
			long epoch = Long.parseLong(timestamp);
			Date d = new Date(epoch*1000);
			//System.out.println(d);
			fw.append(key+"\t"+value+"\t"+username+"\t"+d+"\t"+page_url+"\t"+client+"\t"+session+"\t"+language+"\t"+from+"\t"+user_ip+"\t"+user_agent+"\n");
			
		}
		fw.flush();
		fw.close();
	}

}
