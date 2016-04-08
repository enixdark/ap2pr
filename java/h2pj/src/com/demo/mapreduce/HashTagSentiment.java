package com.demo.mapreduce;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.demo.mapreduce.map.HashTagCountMapper;
import com.demo.mapreduce.reduce.HashTagSentimentReducer;

public class HashTagSentiment extends IMain {
	
	// use data for nlp "http://www.cs.uic.edu/~liub/FBS/opinion-lexicon-English.rar"

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		File dir = new File(args[1]);
		if(dir.exists()){
			FileUtils.deleteDirectory(dir);
		}
		Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.set("job.positivewords.path", args[2]);
        conf.set("job.negativewords.path", args[3]);

        Job job = Job.getInstance(conf);

        job.setJarByClass(HashTagSentiment.class);
        job.setMapperClass(HashTagCountMapper.class);
//        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(HashTagSentimentReducer.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String...args) throws Exception{
		process(HashTagSentiment.class,"resources/tweets.txt","output","resources/positive-words.txt","resources/negative-words.txt");
	}

}
