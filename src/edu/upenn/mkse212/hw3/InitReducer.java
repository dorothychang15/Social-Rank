package edu.upenn.mkse212.hw3;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) 
	throws java.io.IOException, InterruptedException{
		String user = key.toString();
		String rank = "1";
		String friends = "";
		
		// Create a string of the user's friends
		for (Text value : values) {
			if (!value.toString().equals("")) {
				friends += value.toString() + ",";
			}
		}
		// We concatenate the initial rank with a comma delimited list of friends
		context.write(new Text(user), new Text(rank+"-"+friends));
	}
}