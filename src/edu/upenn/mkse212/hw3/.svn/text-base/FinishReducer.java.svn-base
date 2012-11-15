package edu.upenn.mkse212.hw3;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinishReducer extends Reducer<DoubleWritable, Text, Text, Text> {

  /* TODO: Your reducer code here */
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
	throws java.io.IOException, InterruptedException{
		double rankNeg = key.get();
		
		// For each user that matches the rank, print the user followed by rank
		for (Text value : values) {
			String user = value.toString();
			context.write(new Text(user), new Text(Double.toString(-1.0*rankNeg)));
		}
	}
}