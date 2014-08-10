package com.primacy.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.io.IOException;


public class WordCountMapper{

public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	  private final static IntWritable one = new IntWritable(1);
	  private Text word = new Text();
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		StringTokenizer tokenizer  = new StringTokenizer(line);
		
		while (tokenizer.hasMoreTokens()){
			word.set(tokenizer.nextToken());
			output.collect(word, one);
		}
	}

}

public static class Reduce  extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
	int sum = 0;
	while (values.hasNext()){
		sum = sum + values.next().get();
		}
	output.collect(key, new IntWritable(sum));
	}	
}

public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(WordCountMapper.class);
	conf.setJobName("Word Count");
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
	
	conf.setMapperClass(Map.class);
	conf.setReducerClass(Reduce.class);
	conf.setCombinerClass(Reduce.class);
	
	 conf.setInputFormat(TextInputFormat.class);
	 conf.setOutputFormat(TextOutputFormat.class);
	 
	 FileInputFormat.setInputPaths(conf, new Path(args[0]));
	 JobClient.runJob(conf);
}

}