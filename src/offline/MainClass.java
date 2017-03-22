package offline;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass {
	
	// https://unmeshasreeveni.blogspot.com/2014/04/chaining-jobs-in-hadoop-mapreduce.html
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		if (args.length != 2) {
			System.out.printf("Usage: <jar file> <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		String output_path = "intermediate_output";
		Configuration conf = new Configuration();
		
		/*
		 *	Individual word count 
		 */
		
		Job job1=Job.getInstance(conf);
		job1.setJarByClass(MainClass.class);
		
		job1.setMapperClass(WordCountMapper.class);
		job1.setReducerClass(WordCountReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		FileOutputFormat.setOutputPath(job1, new Path(output_path));
		
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		job1.waitForCompletion(true);
		
		/*
		 * 	Global word count by author
		 */
		
		Job job2=Job.getInstance(conf);
		job2.setJarByClass(GlobalCount.class);
		
		job2.setMapperClass(CalculateTFMapper.class);
		job2.setReducerClass(CalculateTFReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job2, new Path(output_path));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		job2.waitForCompletion(true);
	}
}