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

public class WordSizeWordCount {
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable>{        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString(); // Lưu record vào biến string line
            
            // StringTokenizer đưa chuỗi về các từ đơn (token)
            StringTokenizer tokenizer = new StringTokenizer(line); 
            
            // Lặp với các từ vừa tokenized ở record đó và write dạng <key, value>
            while (tokenizer.hasMoreTokens())
            {
                String token = tokenizer.nextToken();
                
                // biến word lưu giá trị độ dài của từ
                IntWritable word = new IntWritable(token.length());
                IntWritable one = new IntWritable(1);
                // context ghi ra key - độ dài từ, value - 1
                context.write(word, one);
            }
        }
    }
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        	
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable x: values){
                sum += x.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
	public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"Word Count Word Size");
        job.setJarByClass(WordSizeWordCount.class);
    
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        Path outputPath = new Path(args[1]);
        
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        outputPath.getFileSystem(conf).delete(outputPath);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
