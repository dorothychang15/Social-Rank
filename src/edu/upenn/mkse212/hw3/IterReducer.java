package edu.upenn.mkse212.hw3;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) 
	throws java.io.IOException, InterruptedException{
		String user = key.toString();
		double totalWeight = 0.0;
		String friends = "";
		
		// Check for received weighted ranks and friends
		for (Text value : values) {
			double weightNegOrFriend = Double.parseDouble(value.toString());
			if (weightNegOrFriend < 0.0) {
				// Value is a ranked weight, negate since negation was just an indicator
				totalWeight += -1.0 * weightNegOrFriend;
			} else {
				// Value is a friend, add to list
				friends += value.toString() + ",";
			}
		}
		// Get new rank with d=0.15
		double newRank = 0.15 + 0.85*totalWeight;
		context.write(new Text(user), new Text(newRank+"-"+friends));
	}
}