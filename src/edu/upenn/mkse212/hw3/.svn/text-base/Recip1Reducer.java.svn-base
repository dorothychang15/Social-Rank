package edu.upenn.mkse212.hw3;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class Recip1Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) 
	throws java.io.IOException, InterruptedException{
		HashMap<String, Integer> friends = new HashMap<String, Integer>();
		
		// Go through users and add to a map
		for (Text value : values) {
			String friend = value.toString();
			if (friends.containsKey(friend)) {
				friends.put(friend, friends.get(friend)+1);
			} else {
				friends.put(friend, 1);
			}
		}
		
		// Iterate over set; if a friend had count > 1, it's a two-way edge
		for (String friend : friends.keySet()) {
			if (friends.get(friend) == 1) {
				context.write(new Text("ONE-WAY"), new Text(""));
			} else {
				context.write(new Text("TWO-WAY"), new Text(""));
			}
		}
	}
	
}