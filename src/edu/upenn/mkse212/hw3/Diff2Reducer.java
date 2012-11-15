package edu.upenn.mkse212.hw3;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class Diff2Reducer extends Reducer<DoubleWritable, Text, Text, Text> {

	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
	throws java.io.IOException, InterruptedException{
		double diffNeg = key.get();
		
		// Write the diff, but negate first since we negated in Diff2Mapper
		context.write(new Text(Double.toString(-1.0*diffNeg)), new Text(""));
	}
	
}