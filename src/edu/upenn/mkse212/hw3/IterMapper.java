package edu.upenn.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		String line = value.toString();
		String[] userRankFriends = line.split("\t");
		String[] rankFriends = userRankFriends[1].split("-");
		
		String user = userRankFriends[0];
		String rank = rankFriends[0];
		
		// Check if user has any friends
		if (rankFriends.length < 2) {
			return;
		}
		String[] friends = rankFriends[1].split(",");
		
		// Get the part inside the summation of the formula we use
		double rankInt = Double.parseDouble(rank);
		double numFriends = friends.length;
		// We negate the weighted rank to indicate it is a rank for the reducer
		String weightedRankNeg = Double.toString(-1.0*rankInt/numFriends);	
		
		for (String friend : friends) {
			// "Send" this user's weighted rank to each of his friends to adjust their rank
			context.write(new Text(friend), new Text(weightedRankNeg));
			// Also send each friend to the user in order to preserve the data
			context.write(new Text(user), new Text(friend));
		}
		
	}
}
