package com.demo.mapreduce.map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import avro.shaded.com.google.common.collect.Sets;

public class HashTagCountMapper extends Mapper<Object, Text, Text, Text>
{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Set<String> positiveWords = null;
	private Set<String> negativeWords = null;
    private Set<String> hashtags = new HashSet<String>();
	private static String BEGIN_COMMENT = ";";
    private String HASHTAG_PATTERN = "(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)";
	
	private HashSet<String> parseWordsList(FileSystem fs, Path wordsListPath) throws IOException{
		HashSet<String> words = Sets.newHashSet();
		if(fs.exists(wordsListPath)){
			FSDataInputStream fi = fs.open(wordsListPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fi));
			String line = null;
			while((line = br.readLine()) != null){
				if(line.length() > 0 && !line.startsWith(BEGIN_COMMENT)){
					words.add(line);
				}
			}
			fi.close();
		}
		return words;
	}
	
	@Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        try {
            FileSystem fs = FileSystem.get(conf);

            String positiveWordsLocation = conf.get("job.positivewords.path");
            String negativeWordsLocation = conf.get("job.negativewords.path");

            positiveWords = parseWordsList(fs, new Path(positiveWordsLocation));
            negativeWords = parseWordsList(fs, new Path(negativeWordsLocation));
            System.out.println(positiveWords);

            //fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(" ") ;
		Integer positiveCount = new Integer(0);
		Integer negativeCount = new Integer(0);
		Integer wordsCount = new Integer(0);
		for (String str: words){
			if (str.matches(HASHTAG_PATTERN)) {
				hashtags.add(str);
			}
			if(positiveWords.contains(str)){
				positiveCount++;
			}
			else if(negativeWords.contains(str)){
				negativeCount++;
			}
			wordsCount++;
		}
		Integer sentimentDifference = 0;
		if (wordsCount > 0) {
			sentimentDifference = positiveCount - negativeCount;
		}
		String stats ;
		for (String hashtag : hashtags) {
			word.set(hashtag);
			stats = String.format("%d %d", sentimentDifference, wordsCount);
			context.write(word, new Text(stats));
		}
		
	}

}
