package com.mr;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class AverageSalary {
	public static class  Map extends Mapper<Object, Text, Text, FloatWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String values[] = value.toString().split(",");
			Text name = new Text((values[1] + values[0]));
			FloatWritable salary = new FloatWritable(Float.parseFloat(values[7]));
			context.write(name, salary);
		}
	}
	
	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
				throws IOException, InterruptedException
		{
			FloatWritable result = new FloatWritable();
			float sum = 0;
			long count = 0;
			for (FloatWritable x: values) {
				sum += x.get();
				count++;
			}
			result.set(sum / count);
			context.write(new Text("Average Salary"), result);
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "AverSal");
		
		job.setJarByClass(AverageSalary.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[2]);
		
		FileInputFormat.addInputPath(job, new Path(args[1])); //change here
        FileOutputFormat.setOutputPath(job, new Path(args[2])); //change here
        
        outputPath.getFileSystem(conf).delete(outputPath);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
