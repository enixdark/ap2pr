package com.demo.mapreduce;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

import com.demo.mapreduce.map.HashTagCountMapper;

public class HashTagCount extends IMain {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		File dir = new File(args[1]);
		if(dir.exists()){
			FileUtils.deleteDirectory(dir);
		}
		Configuration conf = getConf();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf);
		job.setJarByClass(HashTagCount.class);
		job.setMapperClass(HashTagCountMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String...args) throws Exception{
		process(HashTagCount.class,"resources/tweets.txt","output");
	}

}
