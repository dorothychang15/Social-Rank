package edu.upenn.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Diff2Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		String line = value.toString();
		String[] userDiff = line.split("\t");
		double diff = Double.parseDouble(userDiff[1]);
		
		// Negate the diff so the reducer gets them in decreasing order
		context.write(new DoubleWritable(-1.0*diff), new Text(""));
	}

}
