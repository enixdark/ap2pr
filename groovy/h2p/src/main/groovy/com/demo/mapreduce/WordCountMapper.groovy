package com.demo.mapreduce

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
	final static def one = new IntWritable(1)
	def word = new Text()
	public void map(key, value, context) throws IOException, InterruptedException{
		def words = value.toString().split(" ");
		println words
		words.each {
			word.set(it)
			context.write(word,one)
		}
	}
}
