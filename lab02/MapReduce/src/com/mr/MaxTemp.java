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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MaxTemp {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException
		{
			String line = value.toString();
			// tách token từ record
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			
			while(tokenizer.hasMoreTokens()) {
				// year là token đầu tiên
				String year = tokenizer.nextToken();
				// temperature là token tiếp theo
				int temperature = Integer.parseInt(tokenizer.nextToken().trim());
				// ghi ra cặp <year, temperature>
				context.write(new Text(year), new IntWritable(temperature));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{
			int max_temp = 0;
			for (IntWritable x: values) {
				int temp = x.get();
				max_temp = max_temp < temp ? temp :max_temp;
			}
			context.write(key, new IntWritable(max_temp));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "Maximum temperature in year");
		
		job.setJarByClass(MaxTemp.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, new Path(args[0])); //change here
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //change here
        
        outputPath.getFileSystem(conf).delete(outputPath);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
