package edu.upenn.mkse212.hw3;

import java.util.ArrayList;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class Diff1Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) 
	throws java.io.IOException, InterruptedException{
		String user = key.toString();
		ArrayList<String> ranks = new ArrayList<String>();
		
		// Get both ranks
		for (Text value : values) {
			ranks.add(value.toString());
		}
		
		// Get the absolute diff of the two and write as value
		double diff = Double.parseDouble(ranks.get(0)) - Double.parseDouble(ranks.get(1));
		if (diff < 0) {
			diff *= -1.0;
		}
		context.write(new Text(user), new Text(Double.toString(diff)));
	}
	
}