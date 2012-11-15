package edu.upenn.mkse212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Recip2Mapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) 
			  throws IOException, InterruptedException {
		String linkDirection = value.toString().replaceAll("\\s", "");
		
		// Send the link to one key "LINK"
		if (linkDirection.equals("ONE-WAY")) {
			context.write(new Text("LINKS"), new Text(linkDirection));
		}
		else if (linkDirection.equals("TWO-WAY")){
			context.write(new Text("LINKS"), new Text(linkDirection));
		}
	}

}
