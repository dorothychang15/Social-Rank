package edu.upenn.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Diff1Mapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		String line = value.toString();
		
		String[] userRankFriends = line.split("\t");
		String[] rankFriends = userRankFriends[1].split("-");
		
		String user = userRankFriends[0];
		String rank = rankFriends[0];
		
		// Just emit the user and his rank
		context.write(new Text(user), new Text(rank));
	}

}
