package com.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniqueListener {
	
	private enum COUNTERS {
		INVALID_RECORD_COUNT
	}
	
	public static class UniqueListenerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		IntWritable trackId = new IntWritable();
		IntWritable userId = new IntWritable();

		public void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] parts = value.toString().split("[|]");
			
			if (parts.length == 5) {
				userId.set(Integer.parseInt(parts[LastFMConstants.USER_ID]));
				trackId.set(Integer.parseInt(parts[LastFMConstants.TRACK_ID]));
				context.write(trackId, userId);
			} else {
				// add counter for invalid records
				context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
			}
		}
	}

	public static class UniqueListenerReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public void reduce(IntWritable trackId, Iterable<IntWritable> userIds,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			Set<Integer> userIdSet = new HashSet<Integer>();
			for (IntWritable userId : userIds) {
				userIdSet.add(userId.get());
			}
			IntWritable size = new IntWritable(userIdSet.size());
			context.write(trackId, size);
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "Unique Listeners per track");
		job.setJarByClass(UniqueListener.class);
		
		job.setMapperClass(UniqueListenerMapper.class);
		job.setReducerClass(UniqueListenerReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path outputPath = new Path(args[2]);
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		outputPath.getFileSystem(conf).delete(outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		Counters counters = job.getCounters();
		System.out.println("No. of Invalid Records :" + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
	}
}
