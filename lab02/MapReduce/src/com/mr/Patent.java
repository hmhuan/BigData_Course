package com.mr;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Patent {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line, ",");

			while (tokenizer.hasMoreTokens()) {
				Text k = new Text(tokenizer.nextToken());
				Text v = new Text(tokenizer.nextToken());

				context.write(k, v);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, LongWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for (Text value: values)
			{
				sum++;
			}
			context.write(key, new LongWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "patent");
		
		job.setJarByClass(Patent.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[2]);
		
		FileInputFormat.addInputPath(job, new Path(args[1])); //change here
        FileOutputFormat.setOutputPath(job, new Path(args[2])); //change here
        
        //deleting the output path automatically from hdfs so that we don't have delete it explicitly
        outputPath.getFileSystem(conf).delete(outputPath);
        //exiting the job only if the flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
