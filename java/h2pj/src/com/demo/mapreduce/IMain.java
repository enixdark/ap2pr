package com.demo.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public abstract class IMain extends Configured implements Tool {
	public static void process(Class<? extends IMain> class1, String...args) throws Exception{
		int exit = ToolRunner.run(class1.newInstance(), args);
		System.exit(exit);
	}
}
