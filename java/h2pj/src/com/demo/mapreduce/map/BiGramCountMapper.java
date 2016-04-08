package com.demo.mapreduce.map;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BiGramCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	@Override
	public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		Text bigram = new Text();
		String prev = null;
		for(String s: words){
			if(prev != null){
				bigram.set(prev + "\t+\t" + s);
				context.write(bigram, one);
			}
			prev = s;
		}
	}
}
