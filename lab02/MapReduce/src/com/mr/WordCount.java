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

public class WordCount {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1); // Tạo biến IntWritable one có giá trị 1 - value
		private Text word = new Text();  // key

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString(); 
			StringTokenizer tokenizer = new StringTokenizer(line); // tách các từ thuộc mỗi dòng
			
			while (tokenizer.hasMoreTokens()) {
				// Với mỗi từ ghi ra context cặp <từ , 1>
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException 
		{
			int sum = 0; // Khởi tạo biến đếm là 0
			for (IntWritable val : values) 
			{
				sum += val.get(); 
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception 
	{
		// setting một số config
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Word Count");
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class); // set output key là loại Text 
		job.setOutputValueClass(IntWritable.class); // set Output value là loại IntWritable
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, new Path(args[0])); // set path input file
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // set path output file 
		
		outputPath.getFileSystem(conf).delete(outputPath); // delete thư mục output hiện tại nếu đã tồn tại
		
		job.waitForCompletion(true); // job chờ đến khi hoàn thành
	}
}
