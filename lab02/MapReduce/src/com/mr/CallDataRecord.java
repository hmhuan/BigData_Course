package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CallDataRecord {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {
		// Call phone number
 		Text phoneNumber = new Text();
		// Duration of call
		LongWritable durationInMinutes = new LongWritable();
		
 		public void map(Object key, Text value, Mapper<Object, Text, Text, LongWritable>.Context context)
 			throws IOException, InterruptedException {
			// Split a line of call log to to 5 strings by `|`
 			String[] parts = value.toString().split("[|]");
			// In case the call is STD
 			if (parts[CDRConstants.STDFlag].equalsIgnoreCase("1")) {
				// Get call phone number from `parts` list
 				phoneNumber.set(parts[CDRConstants.fromPhoneNumber]);
				// Get call end time from `parts` list
 				String callEndTime = parts[CDRConstants.callEndTime];
				// Get call start time from `parts` list
 				String callStartTime = parts[CDRConstants.callStartTime];
				// Compute duration in millisecond
 				long duration = toMillis(callEndTime) - toMillis(callStartTime);
				// Convert duration to minute unit
 				durationInMinutes.set(duration / (1000 * 60));
				// Sending to output collector which in turn passes the same to reducer
				// <key, value> : <phoneNumber, durationInMinutes>
				context.write(phoneNumber, durationInMinutes);
			}
		}
		
		private long toMillis(String date) {
			// Define the datetime format for `callEndTime`, `callStartTime`
 			SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss");
 			Date dateFrm = null;
 			try {
				// Parse a datatime string with format defined as above
 				dateFrm = format.parse(date);
 			} catch (ParseException e) {
				e.printStackTrace();
 			}
			// Returns the number of milliseconds since January 1, 1970, 00:00:00 GTM
			return dateFrm.getTime();
 		}
 	}
 	
	public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
			// Sum of the duration a phone number made
			long sum = 0;
			// Sum of the duration of each key - phone number
			for (LongWritable val : values) {
				sum += val.get();
			}
			this.result.set(sum);
			// Write to output if sum of the duration is larger than 60 mins
			if (sum >= 60) {
				context.write(key, this.result);
			}
		}
	}
 	
 	public static void main(String[] args)
 		throws Exception {
 		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: stdsubscriber < in>< out>");
			System.exit(2);
		}
 		Job job = new Job(conf, "STD Subscribers");
 		job.setJarByClass(CallDataRecord.class);
 		
 		job.setMapperClass(TokenizerMapper.class);
 		job.setCombinerClass(SumReducer.class);
 		job.setReducerClass(SumReducer.class);
 		
 		job.setOutputKeyClass(Text.class);
 		job.setOutputValueClass(LongWritable.class);
 		
 		FileInputFormat.addInputPath(job, new Path(args[0]));
 		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 		System.exit(job.waitForCompletion(true) ? 0 : 1);
	} 	
}
