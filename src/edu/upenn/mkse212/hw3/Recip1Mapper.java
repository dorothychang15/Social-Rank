package edu.upenn.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Recip1Mapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		String[] users = value.toString().split("\t");
		
		int u1 = Integer.parseInt(users[0]);
		int u2 = Integer.parseInt(users[1]);
		
		// We have the lower user be the key and the higher be a value
		// so that we can group recipocral vertices
		if (u1 < u2) {
			context.write(new Text(Integer.toString(u1)), new Text(Integer.toString(u2)));
		}
		else {
			context.write(new Text(Integer.toString(u2)), new Text(Integer.toString(u1)));
		}
	}

}
