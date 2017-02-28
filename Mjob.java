
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;  
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

//import org.junit.Assert;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.fs.FileSystem;

public class Mjob { 
    public static int K=200; //This is the latent factors used to contain the hidden information.  
	public static int d = 5;  //The number of blocks we use for training each time.
    public static float lambdaW =0.02f; //A constant for training W
    public static float lambdaH =0.02f; //A constant for training H
    public static float zigma = 1.0f;   //A constant for training 
    public static float rowNumber = 138493.0f; //The number of different Users
    public static float colNumber = 26744.0f;  //The number of different movies	
	private static String hdfspath="/ppmf/";  //The path of the HDFS, all files are would be created or modified on this directory. 
	private static String separator= "\t";      //The separator of the input format
	
	//this Mapper are use to create the reducer according to the number of the blocks.
    public static class MjobMap extends  
            Mapper<LongWritable, Text, Text, Text> {  
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {  
        	Configuration conf = context.getConfiguration();
        	String permutation = conf.get("permutation");
        		String item[] = permutation.split(separator);
        		for (int i=0;i<item.length;i++)
        		{
        			Text sequence = new Text();
        			sequence.set(item[i]); 
        			context.write(sequence,new Text("1"));
        		}
        }  
    }  
  
    public static class MjobReduce extends  
            Reducer<Text, Text, Text, Text> {  
    	
    	public void readWH(BufferedReader input, ArrayList<float[]>List) throws IOException{
    		String line="";
    		while ((line = input.readLine())!=null){
    			String item[]=line.split(separator);
                float a[]= new float[K];
                for (int i=0;i<K;i++) a[i]=Float.parseFloat(item[i]);
                List.add(a);
    		}
    	}

        public void readZ(BufferedReader input, HashMap<String, Float> ZMap)throws IOException{
            String line="";
            while ((line = input.readLine())!=null){
                String item[]=line.split(separator);
                // float a[]= new float[K];
                // for (int i=0;i<K;i++) a[i]=Float.parseFloat(item[i]);
                String key = item[1]+"_"+item[2];
                ZMap.put(key,Float.parseFloat(item[3]));
            }

        }
        //the dot product of Wi*Hj, which is the estimation of Zij
    	 public float product(float[] Wi, float[] Hj){
                assert(Wi.length == Hj.length);
                float sum=0;
                for(int k = 0 ; k<Wi.length; k++){
                        sum += Wi[k]*Hj[k];

                }
                return sum;        
        }

        public void reduce(Text key, Iterable<Text> values,  
                Context context) throws IOException, InterruptedException { 
        	
        	
        	String item[]= key.toString().split(",");
        	
        	
        	FileSystem fs=FileSystem.get(context.getConfiguration());
        	try{        	
        	// Z read     	
        	String temp=hdfspath+"Matrix/"+item[0]+"_"+item[1];
            //String temp="/user/hadoop/out/"+item[0]+"_"+item[1]+"-r-00000";
        	Path filePath =new Path(temp);
        	
        	FSDataInputStream fin= fs.open(filePath);
        	BufferedReader input = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
            HashMap<String,Float> ZMap = new HashMap<String,Float>();
            readZ(input,ZMap);
            
            if (input != null) {  
                input.close();  
                input = null;  
            } 
            if (fin != null) {  
                fin.close();  
                fin = null;  
            }   

            // String line=" ";
            
            //
        	// Path filePath2 = new Path("/user/root/out3/"+item[0]+"_"+item[1]+"-r-00001");
        	// FSDataOutputStream fout= fs.create(filePath2);
        	// BufferedWriter output = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));            
            
            //
            // while ((line = input.readLine()) != null) {  
            //  //context.write(key,new Text(line));  
            //  output.write(line+"\n");
            // }
            
            
                    
            
 
            // output.close();
            // fout.close();
        	
            // W read
        	temp=hdfspath+"WMatrix/W"+item[0];
            //temp="/user/hadoop/WMatrix/W"+item[0];
        	Path WPath= new Path(temp);
        	FSDataInputStream Wfin= fs.open(WPath);
        	BufferedReader WInput = new BufferedReader(new InputStreamReader(Wfin, "UTF-8"));
        	ArrayList<float[]> WList= new ArrayList<float[]>();
        	readWH(WInput,WList);
        	WInput.close();
        	Wfin.close();
        	
        	//H read
        	temp=hdfspath+"HMatrix/H"+item[1];
            //temp="/user/hadoop/HMatrix/H"+item[1];
        	Path HPath= new Path(temp);
        	FSDataInputStream Hfin= fs.open(HPath);
        	BufferedReader HInput = new BufferedReader(new InputStreamReader(Hfin, "UTF-8"));
        	ArrayList<float[]> HList= new ArrayList<float[]>();
        	readWH(HInput,HList);
        	HInput.close();
        	Hfin.close();
        	
            //do something for data processing 
            Set<String> keyset = ZMap.keySet();
            

            ArrayList<String> keylist= new ArrayList<String>();
            keylist.addAll(keyset);
            Random ran1 = new Random();
            int N = d;
            

            for (int iter = 0;iter<ZMap.size();++iter){
            //int seleted = ran1.nextInt(N-1);
              int seleted = ran1.nextInt(keylist.size()-1);
              String zkey = keylist.get(seleted);
              float zvalue = ZMap.get(zkey);
              String zitem[]= zkey.split("_");
              int indexi = Integer.parseInt(zitem[0])-1;
              int indexj = Integer.parseInt(zitem[1])-1;
              //convert global index to local index
              int wblock_rows = (int)Math.ceil(rowNumber/d);
              int hblock_cols = (int)Math.ceil(colNumber/d);
          
              indexi = (indexi%wblock_rows);
              indexj = (indexj%hblock_cols);
         
              float error = zvalue - product(WList.get(indexi), HList.get(indexj));
              float Wtempi[] = WList.get(indexi).clone();
              float Htempj[] = HList.get(indexj).clone();
            
            
        	  Configuration conf = context.getConfiguration();
        	
        	  float yita=conf.getFloat("yita",0.05f);

         //   update W and store temporary W[i]
              for (int k = 0 ; k<K; k++){

                //float partial = 1/(zigma*zigma)*(error*HList.get(indexj)[k])+lambdaW*Wtempi[k];
                float partial = (1/(zigma*zigma))*(-error*HList.get(indexj)[k]);
              //  Wtempi[k] -=  yita(partial+lambdaW*Wtempi[k]);
                Wtempi[k] -=  yita*N*(partial+lambdaW*Wtempi[k]);
              }
        //     //update H
              for (int k = 0 ; k<K; k++){

                //float partial = 1/(zigma*zigma)*(error*HList.get(indexj)[k])+lambdaW*Wtempi[k];
                
                float partial = (1/(zigma*zigma))*(-error*WList.get(indexi)[k]);
                //Htempj[k] -=  yita*(partial+lambdaH*Htempj[k]);
                Htempj[k] -=  yita*N*(partial+lambdaH*Htempj[k]);
              }

        //     //recover the W[i]
              WList.set(indexi,Wtempi);
              HList.set(indexj,Htempj);
           }// for iter
            /// .

            ///
            
            //write updated W into hdfs
        	temp=hdfspath+"WMatrix/W"+item[0];
        	WPath= new Path(temp);
        	OutputStream Wfout= fs.create(WPath,true);
        	BufferedWriter WOutput = new BufferedWriter(new OutputStreamWriter(Wfout, "UTF-8"));
            for (int i=0;i<WList.size();i++)
            {
            	float arr[]=WList.get(i);
            	for (int j=0;j<arr.length;j++)
            		WOutput.write(arr[j]+"\t");
            	WOutput.write("\n");
            }
        	WOutput.close();
        	Wfout.close();
        	
        	//
        	temp=hdfspath+"HMatrix/H"+item[1];
        	HPath= new Path(temp);
        	OutputStream Hfout= fs.create(HPath,true);
        	BufferedWriter HOutput = new BufferedWriter(new OutputStreamWriter(Hfout, "UTF-8"));
            for (int i=0;i<HList.size();i++)
            {
            	float arr[]=HList.get(i);
            	for (int j=0;j<arr.length;j++)
            		HOutput.write(arr[j]+"\t");
            	HOutput.write("\n");
            }
        	HOutput.close();
        	Hfout.close();       	
        	}
        	catch (Exception e) {
              System.out.print("no such block");   
        	}
        	
            context.write(key, new Text("1"));
        }  
    }  
  
    public static String PermutaionBuilder(int d) {
		int x[] = new int[d];
		for (int i=0;i<d;i++) x[i]=i;
		String ret = "";
		for (int k=0;k<d;k++){
			Random random = new Random();
			int seleted = random.nextInt(d-1)%(d-k) + k;// generate random integer from k to d-1
		    int temp=x[k];
		    x[k]=x[seleted];
		    x[seleted]=temp;
		    ret += Integer.toString(k)+","+Integer.toString(x[k]);
		    if (k<d-1) ret += "\t";
		}
		return ret;
	}

    
    public static void main(String[] args) throws Exception {
    	float yita0=0.03f;
    	for (int i=1;i<=20;i++)
    	{
    		
        String permutation = PermutaionBuilder(d);      
    	Configuration conf = new Configuration(); 
        conf.set("permutation", permutation);
        float yita=(yita0/((i+1)*1.4f));
        conf.setFloat("yita", yita);
        Job job = new Job(conf);  
        job.setJarByClass(Mjob.class);  
        job.setJobName("Mjob");  
  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
  
        job.setMapperClass(MjobMap.class);  
        job.setReducerClass(MjobReduce.class);  
  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
  
        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path((args[1])+Integer.toString(i)));  
        
        job.setNumReduceTasks(d/2+1);
        job.waitForCompletion(true);
    	}
    }  
} 

