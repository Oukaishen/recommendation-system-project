
import java.io.BufferedWriter;
import java.io.IOException;  





import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;  
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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  

import java.lang.Math;

public class MatrixSpliter {  
  
    private static int d = 5; //The number of blocks we use for training each time
    private static float rowNumber = 943f;  //The number of different Users
    private static float colNumber = 1682f;   //The number of different movies	
	private static String separator= "\t";      //The separator of the input format
	private static String hdfspath="/ppmf/";  //The path of the HDFS, all files are would be created or modified on this directory.
    
	//The mapper is used to put the records to the blocks that they belongs to
    public static class MatrixSpliterMap extends  
            Mapper<LongWritable, Text, Text, Text> {  
              
        public void map(LongWritable key, Text value, Context context)  
                throws IOException, InterruptedException {
        	//extract the record with the user_id, movie_id and the ratings.
            String line = value.toString();  
            String item[] = line.split(separator);
            if (!item[0].equals("userId")) { 
            	//figure out which block the record should be by analyzing the user_id and movie_id.
            	int i=(Integer.parseInt(item[0])-1)/(int)(Math.ceil(rowNumber/d));
                int j=(Integer.parseInt(item[1])-1)/(int)(Math.ceil(colNumber/d));
                String temp=Integer.toString(i)+"_"+Integer.toString(j);
                String temp1= item[0]+"\t"+item[1]+"\t"+item[2];
                Text word = new Text();
                Text outputvalue= new Text();
                word.set(temp);
                outputvalue.set(temp1);                             
                //send the record the blocks they belongs to
	            context.write(word, outputvalue);
            }
        }  
    }  
  
    //This Reducer are used to create the blocks files and write the records to the particular blocks
    public static class MatrixSpliterReduce extends  
            Reducer<Text, Text, Text, Text> {  
    	   	
        public void reduce(Text key, Iterable<Text> values,  
                Context context) throws IOException, InterruptedException { 
        	//create the file for each blocks under the hdfs path. 
        	String name= key.toString();
    		String temp3=hdfspath+"Matrix/"+name;    		
    		FileSystem fs=FileSystem.get(context.getConfiguration());    		
    		Path WPath= new Path(temp3);    
    		OutputStream Wfout= fs.create(WPath,true);
        	BufferedWriter WOutput = new BufferedWriter(new OutputStreamWriter(Wfout, "UTF-8"));           	
        	//writing the records into the blocks they belongs to
        	for (Text val : values)
        	{
               WOutput.write(key.toString()+"\t");
               WOutput.write(val.toString()+"\n");

        	}
            WOutput.close();
        	Wfout.close(); 
        }  
    }  
  
    public static void main(String[] args) throws Exception {  
    	
    	//configure the parameters for MapReduce 
        Configuration conf = new Configuration();  
        Job job = new Job(conf);  
        job.setJarByClass(MatrixSpliter.class);  
        job.setJobName("MatrixSpliter");  
  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
  
        job.setMapperClass(MatrixSpliterMap.class);  
        job.setReducerClass(MatrixSpliterReduce.class);  
  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
  
        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        
        job.waitForCompletion(true);  
    }  
} 

