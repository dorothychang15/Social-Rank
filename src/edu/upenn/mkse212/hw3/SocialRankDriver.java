package edu.upenn.mkse212.hw3;

import java.io.BufferedReader;

import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SocialRankDriver 
{
  public static void main(String[] args) throws Exception 
  {
    /* TODO: Your code here */
	  System.out.println("*** Author: Joseph Chan (joch)");
	  try {
		  String function = args[0];
		  if (function.equals("composite")) {
			  doComposite(args);
		  }
		  else if (function.equals("diff")) {
			  doDiff(args);
		  }
		  else if (function.equals("recip")) {
			  doRecip(args);
		  }
		  else {
			  // Init, iter or finish functions
			  doFunction(args);
		  }
		  System.exit(0);
	  }
	  catch (Exception e) {
		  terminate();
	  }
		 
  }
  
  private static void doComposite(String[] args) throws Exception
  {
	  int diffPeriod = 3;			// Run diff function every diffPeriod iterations
	  double diffTolerance = 30.0;	// Converge when maxDiff is less than diffTolerance
	  
	  String inputDir = args[1];
	  String outputDir = args[2];
	  String interimDir1 = args[3];
	  String interimDir2 = args[4];
	  String diffDir = args[5];
	  String numReducers = args[6];
	  
	  int count = 0;
	  boolean hasConverged = false;
	  
	  // Init
	  String[] initFxn = {"init", inputDir, interimDir1, numReducers};
	  doFunction(initFxn);
	  
	  // Iter while ranks have not converged
	  while(!hasConverged){
		  // Alternate between the two interim directories
		  if (count % 2 == 0) {
			  String[] iterFxn = {"iter", interimDir1, interimDir2, numReducers};
			  doFunction(iterFxn);
		  } else {
			  String[] iterFxn = {"iter", interimDir2, interimDir1, numReducers};
			  doFunction(iterFxn);
		  }
		  
		  // Check diff if we are on a multiple of diffPeriod
		  count++;
		  if (count % diffPeriod == 0) {
			  String[] diffFxn = {"diff", interimDir1, interimDir2, diffDir};
			  doDiff(diffFxn);
			  double maxDiff = readDiffResult(diffDir);
			  if (maxDiff <= diffTolerance) {
				  hasConverged = true;
			  }
		  }
	  }
	  
	  // Finish
	  if (count % 2 == 0) {
		  String[] finishFxn = {"finish", interimDir1, outputDir, "1"};
		  doFunction(finishFxn);
	  } else {
		  String[] finishFxn = {"finish", interimDir2, outputDir, "1"};
		  doFunction(finishFxn);
	  }
  }
  
  private static void doFunction(String[] args) throws Exception
  {   
	  String function = args[0];
	  String inputPath = args[1];
	  String outputPath = args[2];
	  
	  Job job = new Job();
	  job.setJarByClass(SocialRankDriver.class);
	  
	  FileInputFormat.addInputPath(job, new Path(inputPath));
	  deleteDirectory(outputPath);
	  FileOutputFormat.setOutputPath(job, new Path(outputPath));
	  
	  // Set mapper/reducer classes based on function
	  if (function.equals("init")) {
		  job.setMapperClass(InitMapper.class);
		  job.setReducerClass(InitReducer.class);
	  }
	  else if (function.equals("iter")) {
		  job.setMapperClass(IterMapper.class);
		  job.setReducerClass(IterReducer.class);
	  }
	  else if (function.equals("finish")) {
		  job.setMapperClass(FinishMapper.class);
		  job.setReducerClass(FinishReducer.class);
	  }
	  
	  job.setMapOutputKeyClass(Text.class);
	  if (function.equals("finish")) {
		  job.setMapOutputKeyClass(DoubleWritable.class);
	  }
	  job.setMapOutputValueClass(Text.class);
	  
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  
	  int numReducers = Integer.parseInt(args[3]);
	  job.setNumReduceTasks(numReducers);
	  
	  job.waitForCompletion(true);
  }
  
  private static void doDiff(String[] args) throws Exception
  {   
	  String inputPath1 = args[1];
	  String inputPath2 = args[2];
	  String tmpOutputPath = "diffTmp";
	  String outputPath = args[3];
	  
	  // First step of diff gets rank differences for each vertex
	  Job job = new Job();
	  job.setJarByClass(SocialRankDriver.class);
	  
	  FileInputFormat.addInputPath(job, new Path(inputPath1));
	  FileInputFormat.addInputPath(job, new Path(inputPath2));
	  deleteDirectory(tmpOutputPath);
	  FileOutputFormat.setOutputPath(job, new Path(tmpOutputPath));
	  
	  job.setMapperClass(Diff1Mapper.class);
	  job.setReducerClass(Diff1Reducer.class);
	  
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(Text.class);
	  
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  
	  job.setNumReduceTasks(1);
	  
	  boolean diff1Done = job.waitForCompletion(true);
	  if (!diff1Done) terminate();
	  
	  // Second part of diff writes this in descending order of diffs
	  job = new Job();
	  job.setJarByClass(SocialRankDriver.class);
	  
	  FileInputFormat.addInputPath(job, new Path(tmpOutputPath));
	  deleteDirectory(outputPath);
	  FileOutputFormat.setOutputPath(job, new Path(outputPath));
	  
	  job.setMapperClass(Diff2Mapper.class);
	  job.setReducerClass(Diff2Reducer.class);
	  
	  job.setMapOutputKeyClass(DoubleWritable.class);
	  job.setMapOutputValueClass(Text.class);
	  
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  
	  job.setNumReduceTasks(1);
	  
	  job.waitForCompletion(true);
  }

  private static void doRecip(String[] args) throws Exception
  {
	  String inputPath1 = args[1];
	  String tmpOutputPath = "recipTmp";
	  String outputPath = args[2];
	  int numReducers = Integer.parseInt(args[3]);
	  
	  // First part of recip
	  Job job = new Job();
	  job.setJarByClass(SocialRankDriver.class);
	  
	  FileInputFormat.addInputPath(job, new Path(inputPath1));
	  deleteDirectory(tmpOutputPath);
	  FileOutputFormat.setOutputPath(job, new Path(tmpOutputPath));
	  
	  job.setMapperClass(Recip1Mapper.class);
	  job.setReducerClass(Recip1Reducer.class);
	  
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(Text.class);
	  
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  
	  job.setNumReduceTasks(numReducers);
	  
	  boolean diff1Done = job.waitForCompletion(true);
	  if (!diff1Done) terminate();
	  
	  // Second part of recip
	  job = new Job();
	  job.setJarByClass(SocialRankDriver.class);
	  
	  FileInputFormat.addInputPath(job, new Path(tmpOutputPath));
	  deleteDirectory(outputPath);
	  FileOutputFormat.setOutputPath(job, new Path(outputPath));
	  
	  job.setMapperClass(Recip2Mapper.class);
	  job.setReducerClass(Recip2Reducer.class);
	  
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(Text.class);
	  
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  
	  job.setNumReduceTasks(numReducers);
	  
	  job.waitForCompletion(true);
  }
  
  private static void terminate() {
	  System.err.println("Usage:\n" +
		  		"SocialRankDriver init <inputDir> <outputDir> <#reducers>\n" +
		  		"SocialRankDriver iter <inputDir> <outputDir> <#reducers>\n" +
		  		"SocialRankDriver diff <inputDir1> <inputDir2> <outputDir>\n" +
		  		"SocialRankDriver finish <inputDir> <outputDir> <#reducers>");
	  System.exit(-1);
  }
  
  // Given an output folder, returns the first double from the first part-r-00000 file
  static double readDiffResult(String path) throws Exception 
  {
    double diffnum = 0.0;
    Path diffpath = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(path),conf);
    
    if (fs.exists(diffpath)) {
      FileStatus[] ls = fs.listStatus(diffpath);
      for (FileStatus file : ls) {
	if (file.getPath().getName().startsWith("part-r-00000")) {
	  FSDataInputStream diffin = fs.open(file.getPath());
	  BufferedReader d = new BufferedReader(new InputStreamReader(diffin));
	  String diffcontent = d.readLine();
	  diffnum = Double.parseDouble(diffcontent);
	  d.close();
	}
      }
    }
    
    fs.close();
    return diffnum;
  }

  static void deleteDirectory(String path) throws Exception {
    Path todelete = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(path),conf);
    
    if (fs.exists(todelete)) 
      fs.delete(todelete, true);
      
    fs.close();
  }

}
