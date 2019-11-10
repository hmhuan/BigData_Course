package com.mr;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class WeatherData {
	public static class TemperatureMapper extends  Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			// Lấy giá trị Date trong record 
			String date = line.substring(6, 14);
			// Lấy giá trị max_temp (theo header file là từ 39 - 45)
			float max_temp = Float.parseFloat(line.substring(39, 45).trim());
			// Lấy giá trị min_temp (theo header file là từ 47 - 53)
			float min_temp = Float.parseFloat(line.substring(47, 53).trim());
			// ghi ra context theo điều kiện đề bài
			if (max_temp > 40.0) {
				context.write(new Text("Hot Day " + date), new Text(String.valueOf(max_temp)));
			}
			if (min_temp < 10.0) {
				context.write(new Text("Cold Day " + date), new Text(String.valueOf(min_temp)));
			}
		}
	}
	public static class TemperatureReducer extends  Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterator<Text> Values, Context context) throws IOException, InterruptedException{
			
			String temperature = Values.next().toString();
			context.write(new Text(key), new Text(temperature));
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Temperature Day");
		job.setJarByClass(WeatherData.class);
		
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class); // đặt Output key class tương ứng là Text
	    job.setOutputValueClass(Text.class); // đặt Output value class tương ứng là Text
		
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        Path outputPath = new Path(args[1]);
        
        FileInputFormat.addInputPath(job, new Path(args[0])); //change here
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //change here
        
        outputPath.getFileSystem(conf).delete(outputPath);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
