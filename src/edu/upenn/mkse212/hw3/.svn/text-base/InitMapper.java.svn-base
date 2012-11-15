package edu.upenn.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		String line = value.toString();
		String[] friendships = line.split("\t");
		String user = friendships[0];
		String friend = friendships[1];
		
		// Emit key of user with friend as value
		context.write(new Text(user), new Text(friend));
		// Do this to ensure that users with no friends, but who are friends of others,
		// are passed on to the reducers
		context.write(new Text(friend),	new Text(""));
	}
}
