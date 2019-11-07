package com.mr;

import java.io.IOException;

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


public class SkippedOnRadioTrack {
	private enum COUNTERS {
		INVALID_RECORD_COUNT
	}

	public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			IntWritable trackId = new IntWritable();
			IntWritable radio = new IntWritable();
			IntWritable skipped = new IntWritable();
			String[] parts = value.toString().split("[|]");

			int r = Integer.parseInt(parts[LastFMConstants.RADIO]);
			int s = Integer.parseInt(parts[LastFMConstants.IS_SKIPPED]);
			radio.set(r);
			skipped.set(s);
			trackId.set(Integer.parseInt(parts[LastFMConstants.TRACK_ID]));

			if (parts.length == 5) {
				if  (r + s == 2)
					context.write(trackId, new IntWritable(1));
				else
					context.write(trackId, new IntWritable(0));
			} else {
				context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
			}
		}
	}

	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable x : values) {
				sum += x.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Number of times skipped on radio per track");

		job.setJarByClass(SkippedOnRadioTrack.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		Path outputPath = new Path(args[2]);
		
		// String is_on_Radio = args[3];

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		outputPath.getFileSystem(conf).delete(outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		Counters counters = job.getCounters();
		System.out.println("No. of Invalid Records :" + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
	}
}
