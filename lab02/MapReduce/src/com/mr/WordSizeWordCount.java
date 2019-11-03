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
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
        private static IntWritable count;
        private Text word = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            // Convert the record (single line) to String and storing it in a String variable line 
            String line = value.toString(); 
            
            // StringTokenizer is break the line into words
            StringTokenizer tokenizer = new StringTokenizer(line);
            
            // iterating through all the words available in that line and forming the key-value pair
            while (tokenizer.hasMoreTokens())
            {
                String token = tokenizer.nextToken();
                
                // finding the length of each token(word)
                count = new IntWritable(token.length());
                word.set(token);
                
                //Sending to output collector which in turn passes the same to reducer
                //So in this case the output from mapper will be the length of a word and that word
                context.write(count, word);
            }
        }
    }
    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, IntWritable>{
        	
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (Text x: values){
                sum++;
            }
            context.write(key, new IntWritable(sum));
        }
    }
	public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"WordSize");
        job.setJarByClass(WordSizeWordCount.class);
    
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        Path outputPath = new Path(args[2]);
        
        FileInputFormat.addInputPath(job, new Path(args[1])); //change here
        FileOutputFormat.setOutputPath(job, new Path(args[2])); //change here
        
        outputPath.getFileSystem(conf).delete(outputPath);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
