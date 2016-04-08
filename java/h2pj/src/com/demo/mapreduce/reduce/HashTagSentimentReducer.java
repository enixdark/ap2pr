package com.demo.mapreduce.reduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HashTagSentimentReducer extends Reducer<Text,Text,Text,DoubleWritable> {
	@Override
	public void reduce(Text key,Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
		double totalDifference = 0;
		double totalWords = 0;
		for (Text val : values) {
			String[] parts = val.toString().split(" ") ;
			totalDifference += Double.parseDouble(parts[0]) ;
			totalWords += Double.parseDouble(parts[1]) ;
		}
		context.write(key, new DoubleWritable(totalDifference/totalWords));
	};
}
