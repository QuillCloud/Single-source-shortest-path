package comp9313.ass2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class SingleSourceSP {
	// My counter UPDATE
	enum Mycounter {
		UPDATE
	}
	
	// store input path and output path from arguments
    public static String OUT = "output";
    public static String IN = "input";
    
    // Mapper for converting the input file to the desired format for iteration
    public static class Pre_Mapper extends Mapper<Object, Text, Text, Text> {
    	// node store the key, content store the value
    	private Text node = new Text();
    	private Text content = new Text();
    	
    	 @Override
         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		 String[] sp1 = value.toString().split("\\s+");
    		 // the key is node id
    		 node.set(sp1[1]);
    		 // the value is adjacency node and the distance, eg "1:2.0"
    		 content.set(sp1[2] + ":" + sp1[3]);
    		 context.write(node, content);
    	 }    	
    }
    
    // Reducer for converting the input file to the desired format for iteration
    public static class Pre_Reducer extends Reducer<Text, Text, Text, Text> {
    	// content store the value
    	private Text content = new Text();
    	
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	String c = "";
        	//combine the adjacency node as a single string, eg "1:2.0 2:4.0"
        	for (Text val : values) {
        		c += val.toString() + " ";
        	}
        	// add space in case this node has no adjacent node
        	c += " ";
        	// get the target node name
        	String target = context.getConfiguration().get("target");
        	// check if current key is target node
        	// if it is target node, then initialize the distance to 0 and the path as itself
        	// else initialize the distance to infinity and the path as 'null'
        	if (target.equals(key.toString()))
        		c = "0.0|" + target + "|" + c;
        	else
        		c = "Infinity" + "|null" + "|" + c;
        	// the key is node name
        	// the value is distance,path and adjacency list joined by '|', eg "0.0|0|2:5.0 1:10.0" 
        	content.set(c);
        	context.write(key, content);
        }
    }
    
    // Mapper for iterator
    public static class SSSPMapper extends Mapper<Object, Text, LongWritable, Text> {
    	// node store the key, content store the value
       private LongWritable node = new LongWritable();
       private Text content = new Text();
        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	// split by '|', get 3 parts
        	String[] sp1 = value.toString().split("\\|");
        	// split first part by spaces to get node id and distance 
        	String[] sp1_0 = sp1[0].split("\\s+");
        	// store current node distance, initialize as infinity
        	double base = Double.MAX_VALUE;
        	// the updated value to write, initialize as Infinity
        	String update_value = "Infinity";
        	//  write process, key is node id
        	// the value is distance, path and adjacency list joined by '|', eg "0.0|0|2:5.0 1:10.0" 
        	node.set(Integer.parseInt(sp1_0[0]));
        	content.set(sp1_0[1] + "|" + sp1[1] + "|" + sp1[2]);
        	context.write(node, content);
        	// split third part by spaces to get each node in adjacency list
        	String[] sp1_2 = sp1[2].split("\\s+");
        	// if current node distance is not infinity, store it as base value
        	if (!sp1_0[1].equals("Infinity"))
        		base = Double.parseDouble(sp1_0[1]);
        	// go through each adjacency node
        	for (int i = 0; i < sp1_2.length; i++) {
        		// split by ':' to get adjacency node id and its distance from current node
        		String[] node_value = sp1_2[i].split(":");
        		// write process, key is adjacency node id
        		node.set(Integer.parseInt(node_value[0]));
        		// updated distance by plus base value with adjacency node's distance from current node
        		if (base != Double.MAX_VALUE) 
        			update_value = Double.toString(base + Double.parseDouble(node_value[1]));
        		// combine the updated value and current node's path as value
        		content.set(update_value + "|" + sp1[1]);
        		context.write(node, content);
        	}
        	
        }

    }
    // Reducer for iterator
    public static class SSSPReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    	// content store the value
    	private Text content = new Text();
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	// original is the distance of current node
        	double original = -1;
        	double temp = -1;
        	// shortest_distance store the shortest distance of current node, initialize as infinity
        	double shortest_distance = Double.MAX_VALUE;
        	// the path_length to write, initialize as infinity
        	String path_length = "Infinity";
        	// store the adjacency list
        	String adj_node = "";
        	// store the path
        	String path = "null";
        	for (Text val : values) {
        		// split by '|' to get distance, path. and one of them can get extra part which is adjacency list 
        		String[] sp = val.toString().split("\\|");
        		if (sp[0].equals("Infinity")) {
    				temp = Double.MAX_VALUE;
    			} else {
    				temp = Double.parseDouble(sp[0]);
    			}
        		// if have third part which means get the adjacency list
        		if (sp.length > 2) {
        			
        			// its distance is current node original distance
        			original = temp;
        			// store adjacency list
        			adj_node = sp[2];
        			// if distance is smaller than current shortest_distance value
        			if (shortest_distance > temp) {
        				// update shortest distance
        				shortest_distance = temp;
        				// set path to original path
        				path = sp[1];
        			}
        		} else {
        			// if distance is smaller than current shortest_distance value
	        		if (shortest_distance > temp) {
	        			// update shortest distance
	        			shortest_distance = temp;
	        			// update path
	        			path = sp[1] + "->" + key.toString();
	    			}
        		}
        	}
        	// if shortest distance is not the original distance, means the node is updated, the count will increase by 1
        	if (original != shortest_distance)
        		context.getCounter(Mycounter.UPDATE).increment(1);
        	// add a space to adj_node in case this node has no adjacent node
        	adj_node += " ";
        	// change path_length if shortest_distance is not inifinty
        	if (shortest_distance != Double.MAX_VALUE)
        		path_length = Double.toString(shortest_distance);
        	// the key is node id
        	// the value is (updated) distance,(updated) path and adjacency list joined by '|', eg "0|0.0|2:5.0 1:10.0" 
        	content.set(path_length + "|" + path + "|" + adj_node);
        	context.write(key, content);
        }
    }
    
    // Mapper for convert the output to our final desired format
    public static class Post_Mapper extends Mapper<Object, Text, LongWritable, Text> {
    	// node store the key, content store the value
    	private LongWritable node = new LongWritable();
    	private Text content = new Text();
    	
    	@Override
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		// split to get node id its distance, path, adjacency list
    		String[] sp1 = value.toString().split("\\|");	
    		// only keep the node that has a path to target
    		if (!sp1[1].equals("null")) {
    			String[] sp1_1 = sp1[0].split("\\s+");
    			// write process, key is node id
    			// value is distance and path join by '\t' eg "4.0	1->2->4" 
    			node.set(Integer.parseInt(sp1_1[0]));
    			content.set(sp1_1[1] + "\t" + sp1[1]);
    			context.write(node, content);
    		}
    	}
    }
    
    // Reducer for convert the output to our final desired format
    public static class Post_Reducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    	// content store the value
    	private Text content = new Text();
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	//get the value and write key and value with out change
        	for (Text val : values) {
        		content = val;
        	}
        	context.write(key, content);
        }
    }

    public static void main(String[] args) throws Exception {        

        IN = args[0];

        OUT = args[1];

        int iteration = 0;

        String input = IN;

        String output = OUT + iteration;
        
        // convert the input file to the desired format for iteration
        // this part set a parameter 'target' to pass the target node name
        Configuration conf_pre = new Configuration();
        conf_pre.set("target", args[2]);
   		Job job_pre = Job.getInstance(conf_pre, "Pre-Operation");
   		job_pre.setJarByClass(SingleSourceSP.class);
   		job_pre.setMapperClass(Pre_Mapper.class);
   		job_pre.setReducerClass(Pre_Reducer.class);
   		job_pre.setOutputKeyClass(Text.class);
   		job_pre.setOutputValueClass(Text.class);
   		FileInputFormat.addInputPath(job_pre, new Path(input));
   		FileOutputFormat.setOutputPath(job_pre, new Path(output));
   		job_pre.waitForCompletion(true);
   		
   		input = output;    

        iteration ++;

        output = OUT + iteration;
        
        boolean isdone = false;
        while (isdone == false) {
        	// iterator mapreduce, use SSSPMapper and SSSPReducer
	        Configuration conf = new Configuration();
	   		Job job = Job.getInstance(conf, "SingleSourceSP Iterator");
	   		job.setJarByClass(SingleSourceSP.class);
	   		job.setMapperClass(SSSPMapper.class);
	   		job.setReducerClass(SSSPReducer.class);
	   		job.setOutputKeyClass(LongWritable.class);
	   		job.setOutputValueClass(Text.class);
	   		FileInputFormat.addInputPath(job, new Path(input));
	   		FileOutputFormat.setOutputPath(job, new Path(output));
	   		job.waitForCompletion(true);
        	
	   		// delete the previous input temporary folder
	   		
        	Path folder_delete = new Path(input);
        	FileSystem hdfs_delete = FileSystem.get(new URI(input), new Configuration());
        	if (hdfs_delete.exists(folder_delete))
        		hdfs_delete.delete(folder_delete, true);
        	input = output;    

            iteration ++;       

            output = OUT + iteration;
   
            // the counter store the number of note that has updated
        	// if no node has updated, then terminate the loop
	   		Counters counter = job.getCounters();
        	Counter cn = counter.findCounter(Mycounter.UPDATE);
            if(cn.getValue() == 0){
            	isdone = true;
            }
        }
        // use Post_Mapper and Post_Reducer to convert output to the desired format
        Configuration conf_post = new Configuration();
   		Job job = Job.getInstance(conf_post, "Post-Operation");
   		job.setNumReduceTasks(1);
   		job.setJarByClass(SingleSourceSP.class);
   		job.setMapperClass(Post_Mapper.class);
   		job.setReducerClass(Post_Reducer.class);
   		job.setOutputKeyClass(LongWritable.class);
   		job.setOutputValueClass(Text.class);
   		FileInputFormat.addInputPath(job, new Path(input));
   		FileOutputFormat.setOutputPath(job, new Path(OUT));
   		job.waitForCompletion(true);
   		
   		// delete the previous input temporary folder
   		Path folder_delete = new Path(input);
    	FileSystem hdfs_delete = FileSystem.get(new URI(input), new Configuration());
    	if (hdfs_delete.exists(folder_delete))
    		hdfs_delete.delete(folder_delete, true);
    	
    }

}

