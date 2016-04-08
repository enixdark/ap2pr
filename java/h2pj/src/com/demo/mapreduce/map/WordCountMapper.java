package com.demo.mapreduce.map;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Lists;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
	final static IntWritable one = new IntWritable(1);
	Text word = new Text();
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		String[] words = value.toString().split(" ");
		Arrays.asList(words).stream().forEach( it -> {
			word.set(it);
			try {
				context.write(word,one);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
	}
}
