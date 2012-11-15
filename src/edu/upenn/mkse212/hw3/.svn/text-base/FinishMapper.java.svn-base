package edu.upenn.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinishMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		String line = value.toString();
		String[] userRankFriends = line.split("\t");
		String[] rankFriends = userRankFriends[1].split("-");
		
		String user = userRankFriends[0];
		double rank = Double.parseDouble(rankFriends[0]);
		
		// We negate the rank so that the reducer gets them in decreasing order
		context.write(new DoubleWritable(-1.0*rank), new Text(user));
	}
}
