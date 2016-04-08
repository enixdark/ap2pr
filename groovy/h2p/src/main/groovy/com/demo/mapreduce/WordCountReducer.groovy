package com.demo.mapreduce

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	public void reduce(key, values, context) throws IOException, InterruptedException{
		int total = 0
		values.each { total++ }
		context.write(key, new IntWritable(total))
	}
}
