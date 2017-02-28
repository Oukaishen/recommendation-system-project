
import java.io.BufferedWriter;
import java.io.IOException;  
//import java.util.StringTokenizer;  
  
import java.util.Random;


import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;   
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  

import java.lang.Math;

public class WHSpliter {  
  
	private static int K=200; //This is the latent factors used to contain the hidden information.  
    private static int d=5; //The number of blocks we use for training each time.
    private static float rowNumber = 138493; //The number of different Users
    private static float colNumber = 26744;  //The number of different movies	    
	private static String hdfspath="/ppmf/"; //The path of the HDFS, all files are would be created or modified on this directory.  
    
	
    public static class WHSpliterMap extends  
            Mapper<LongWritable, Text, Text, Text> {  
    	
        public void map(LongWritable key, Text value, Context context)  
                throws IOException, InterruptedException {      
        	context.write(new Text("1"), new Text("1"));
        }  
    }  
  
    public static class WHSpliterReduce extends  
            Reducer<Text, Text, Text, Text> {  


        
        public void reduce(Text key, Iterable<Text> values,  
                Context context) throws IOException, InterruptedException { 
        	
        	int ki=(int)(Math.ceil(rowNumber/d));
            int kj=(int)(Math.ceil(colNumber/d)); 
            FileSystem fs=FileSystem.get(context.getConfiguration());
        	for (int k=0;k<d;k++)
        	{
            	String temp=hdfspath+"WMatrix/W"+Integer.toString(k);
            	Path filePath =new Path(temp);
            	FSDataOutputStream fout= fs.create(filePath);
            	BufferedWriter output = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
        		for (int i=0;i<ki;i++)
        		{
        			if (k*ki+i>=rowNumber) break;
        				for (int p=1;p<=K;p++){
                            Random random = new Random();

        					output.write(random.nextFloat()/Math.sqrt(K)+"\t");
                        }
        				output.write("\n");
        			
        		}
                output.close();
                fout.close();
        		
            	String temp2=hdfspath+"HMatrix/H"+Integer.toString(k);
            	Path filePath2 =new Path(temp2);
            	FSDataOutputStream fout2= fs.create(filePath2);
            	BufferedWriter output2 = new BufferedWriter(new OutputStreamWriter(fout2, "UTF-8"));
        		for (int i=0;i<kj;i++)
        		{
        			if (k*kj+i>=colNumber) break;

        				for (int p=1;p<=K;p++){
        					Random random = new Random();

                            output2.write(random.nextFloat()/Math.sqrt(K)+"\t");
                        }
        				output2.write("\n");        				        				
        			
        		}   
                output2.close();
                fout2.close();
        	}
        	context.write(new Text("1"), new Text("1"));
        }  
    }  
  
    public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();  
        Job job = new Job(conf);  
        job.setJarByClass(WHSpliter.class);  
        job.setJobName("WHSpliter");  
  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
  
        job.setMapperClass(WHSpliterMap.class);  
        job.setReducerClass(WHSpliterReduce.class);  
  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
  
        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
        
        
        job.waitForCompletion(true);  
    }  
} 

