//import package and library
package com.mr;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class DeIdentifyHealthcare {
    static Logger logger = Logger.getLogger(DeIdentifyHealthcare.class.getName());
    
    // Sử dụng các cột 2, 3, 4, 5, 6, 8
    // 11111|bbb1|3/29/1995|4494428023|bbb1@xxx.com|90922865|F|HIV|84
    public static Integer[] encryptCol = {2, 3, 4, 5, 6, 8};
    
    // tạo key để encrypt
    private static byte[] key_07 = new String("key07").getBytes(); 
    
    //Mapper
    public static class Map extends Mapper < Object, Text, NullWritable, Text > {
        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
            //value = PatientID,Name,DOB,Phone Number,Email_Address,SSN,Gender,Disease,weight
            //convert records to string and breaking line into word
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), "[|]");

            //Create Arraylist and add encryptCol to list, then list=2,3,4,5,6,8 
            List < Integer > list = new ArrayList < Integer > ();
            Collections.addAll(list, encryptCol); 
            //System.out.println("Mapper one " + value);
            
            String newStr = "";
            int counter = 1;
            //iterating through all the words available in that line.
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                
                //get the list and token with key_07 and save to variable newStr
                if (list.contains(counter)) {
                    if (newStr.length() > 0)
                        newStr += ",";
                    newStr += encrypt(token, key_07);
                } else {
                    if (newStr.length() > 0)
                        newStr += ",";
                    newStr += token;
                }
                counter++;
            }
            context.write(NullWritable.get(), new Text(newStr.toString()));
        }
    }

    //Main
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }

        //Mapper's output types are not default so we have to define the following properties
        Configuration conf = new Configuration();
        //reads the default configuration of cluster from the configuration files
        Job job = Job.getInstance(conf, "De Indentify Healthcare data");
        //Defining Jar by class
        job.setJarByClass(DeIdentifyHealthcare.class);
        
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        //Defining the output key class for the final  i.e. from reduce
        job.setOutputKeyClass(NullWritable.class);

        //Defining the output value class for the final output i.e. from reduce
        job.setOutputValueClass(Text.class);

        //Defining the mapper class name
        job.setMapperClass(Map.class);

        //Defining input Format class which is responsible to parse the dataset into a key value pair
        job.setInputFormatClass(TextInputFormat.class);
        //Defining output Format class which is responsible to parse the final key-value output from MR framework to a text file into the hard disk
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputPath = new Path(args[1]);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        outputPath.getFileSystem(conf).delete(outputPath);
         
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    //Encrypt
    public static String encrypt(String strToEncrypt, byte[] key) {
        try {
            //Decrypt with AES/ECB/PKCS5Padding
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            
            //Construct a SecretKey from byte array key, 
            SecretKeySpec secretKey = new SecretKeySpec(key, "AES");

            //Initializes this cipher with secretKey.
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            
            //Encoding string
            String encryptedString = Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes()));
            
            return encryptedString.trim();
        } 
        catch (Exception e) {
            logger.error("Error while encrypting", e);
        }
        return null;
    }
}