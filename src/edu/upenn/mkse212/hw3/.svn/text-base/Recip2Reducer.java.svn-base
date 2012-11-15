package edu.upenn.mkse212.hw3;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class Recip2Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) 
	throws java.io.IOException, InterruptedException{
		int totalLinks = 0;
		int twoWayCount = 0;
		
		// Count occurrences of bidirectional links and total num of links
		for (Text value : values) {
			String linkDirection = value.toString();
			if (linkDirection.equals("TWO-WAY")){
				twoWayCount++;
			}
			totalLinks++;
		}
		
		// Divide to get reciprocity
		double reciprocity = (double)twoWayCount/(double)totalLinks;
		context.write(new Text(Double.toString(reciprocity)), new Text(""));
	}
	
}