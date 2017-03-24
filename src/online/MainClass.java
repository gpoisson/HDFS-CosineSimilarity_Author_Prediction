package online;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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
		
		Configuration conf = new Configuration();
		
		// Create attribute vector
		
		Job job1=Job.getInstance(conf);
		job1.setJarByClass(MainClass.class);
		
		job1.setMapperClass(NewDocAAVMapper.class);
		job1.setReducerClass(NewDocAAVReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));	
		FileOutputFormat.setOutputPath(job1, new Path("data/word_count/"));
		
		job1.waitForCompletion(true);
		
		/*
		
		//	Individual word/author count 

		Job job1=Job.getInstance(conf);
		job1.setJarByClass(MainClass.class);
		
		job1.setMapperClass(WordCountMapper.class);
		job1.setReducerClass(WordCountReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));	
		FileOutputFormat.setOutputPath(job1, new Path("data/word_count/"));
		
		job1.waitForCompletion(true);
		
		
		// 	Calculate TF
				
		Job job2=Job.getInstance(conf);
		job2.setJarByClass(MainClass.class);
		
		job2.setMapperClass(CalculateTFMapper.class);
		job2.setReducerClass(CalculateTFReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job2, new Path("data/word_count/"));
		FileOutputFormat.setOutputPath(job2, new Path("data/tf/"));
		
		job2.waitForCompletion(true);
	
		
		// 	Author count
		
		Job job3=Job.getInstance(conf);
		job3.setJarByClass(MainClass.class);
		
		job3.setMapperClass(AuthorCountMapper.class);
		//job3.setCombinerClass(AuthorCountCombiner.class);
		job3.setReducerClass(AuthorCountReducer.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job3, new Path("data/word_count/"));
		FileOutputFormat.setOutputPath(job3, new Path("data/author_count/"));
		
		job3.waitForCompletion(true);
		
		// 	Number of authors using a word
		
		Job job4=Job.getInstance(conf);
		job4.setJarByClass(MainClass.class);
		
		job4.setMapperClass(AuthorWordUseCountMapper.class);
		job4.setReducerClass(AuthorWordUseCountReducer.class);
		
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job4, new Path("data/word_count/"));
		FileOutputFormat.setOutputPath(job4, new Path("data/author_word_use_count/"));
				
		job4.waitForCompletion(true);
		
		// 	Calculate IDF
		
		Job job5=Job.getInstance(conf);
		job5.setJarByClass(MainClass.class);
		
		//job5.addCacheFile(new Path("data/author_word_use_count/").toUri());
		
		job5.setMapperClass(CalculateIDFMapper.class);
		job5.setReducerClass(CalculateIDFReducer.class);
		
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		
		//job5.setInputFormatClass(TextInputFormat.class);
		//job5.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job5, new Path("data/author_word_use_count/"));
		//MultipleInputs.addInputPath(job5, new Path("data/author_count/"), TextInputFormat.class);
		//MultipleInputs.addInputPath(job5, new Path("data/author_word_use_count/"), TextInputFormat.class);
		FileOutputFormat.setOutputPath(job5, new Path("data/idf/"));
		
		job5.waitForCompletion(true);
		
		// 	Calculate AAVs
		
		Job job6=Job.getInstance(conf);
		job6.setJarByClass(MainClass.class);
		
		job6.setMapperClass(CalculateAAVMapper.class);
		job6.setReducerClass(CalculateAAVReducer.class);
		
		job6.setOutputKeyClass(Text.class);
		job6.setOutputValueClass(Text.class);
		
		job6.setInputFormatClass(TextInputFormat.class);
		job6.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job6, new Path("data/idf/"), TextInputFormat.class);
		MultipleInputs.addInputPath(job6, new Path("data/tf/"), TextInputFormat.class);
		FileOutputFormat.setOutputPath(job6, new Path(args[1]));
		
		job6.waitForCompletion(true);
		
		*/
	}
}