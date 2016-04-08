package com.demo.mapreduce

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner


class WordCountConfigure extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
//		try{
			File dir = new File(args[1])
		
			if(dir.exists()){
				dir.delete()
			}
			Configuration conf = getConf()
//			conf.set("fs.defaultFS", "hdfs://localhost:9000");
//			conf.set("mapred.job.tracker", "localhost:9001");
//			conf.set("mapreduce.jobtracker.address", "localhost:54311");
//			conf.set("mapreduce.framework.name", "yarn");
//			conf.set("yarn.resourcemanager.address", "localhost:8032");
			
			args = new GenericOptionsParser(conf, args).getRemainingArgs()
			println args
			
			Job job = new Job(conf)//Job.getInstance(conf)
			job.setJobName("word count")
//			job.setJar("h2p-1.0-SNAPSHOT.jar")
			job.setJarByClass(WordCountConfigure.class)
			job.setMapperClass(WordCountMapper.class)
			job.setReducerClass(WordCountReducer.class)
			job.setOutputKeyClass(Text.class)
			job.setOutputValueClass(IntWritable.class)
			
			FileInputFormat.addInputPath(job, new Path(args[0]))
			FileOutputFormat.setOutputPath(job, new Path(args[1]))
			return job.waitForCompletion(true) ? 0 : 1
//		}catch(e){
//			e.printStackTrace()
//		}
		
	}
	
	static def mapreduceProcess(String...args){
		def exit = ToolRunner.run(new WordCountConfigure(), args)
		println exit
		System.exit(exit)
	}
	static main(args) {
		mapreduceProcess("resources/tweets.txt","ouput")
		println WordCountMapper.class
	}

}
