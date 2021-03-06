package online;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import offline.AuthorCountMapper;
import offline.AuthorCountReducer;
import offline.AuthorNamesMapper;
import offline.AuthorNamesReducer;
import offline.AuthorWordUseCountMapper;
import offline.AuthorWordUseCountReducer;
import offline.CalculateAAVMapper;
//import offline.CalculateAAVReducer;
import offline.CalculateIDFMapper;
import offline.CalculateIDFReducer;
import offline.CalculateTFMapper;
import offline.CalculateTFReducer;
import offline.WordCountMapper;
import offline.WordCountReducer;

public class MainClass {
	
	// https://unmeshasreeveni.blogspot.com/2014/04/chaining-jobs-in-hadoop-mapreduce.html
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		if (args.length != 2) {
			System.out.printf("Usage: <jar file> <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		
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
		FileOutputFormat.setOutputPath(job1, new Path("mystery_data/word_count/"));
		
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
		
		FileInputFormat.setInputPaths(job2, new Path("mystery_data/word_count/"));
		FileOutputFormat.setOutputPath(job2, new Path("mystery_data/tf/"));
		
		job2.waitForCompletion(true);
		
		// 	Author count
		
		Job job3=Job.getInstance(conf);
		job3.setJarByClass(MainClass.class);
		
		job3.setMapperClass(AuthorCountMapper.class);
		job3.setReducerClass(AuthorCountReducer.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job3, new Path("mystery_data/word_count/"));
		FileOutputFormat.setOutputPath(job3, new Path("mystery_data/author_count/"));
		
		job3.waitForCompletion(true);
		
		// 	Author names
		
		Job job3_2=Job.getInstance(conf);
		job3_2.setJarByClass(MainClass.class);
			
		job3_2.setMapperClass(AuthorNamesMapper.class);
		job3_2.setReducerClass(AuthorNamesReducer.class);
			
		job3_2.setOutputKeyClass(Text.class);
		job3_2.setOutputValueClass(Text.class);
			
		job3_2.setInputFormatClass(TextInputFormat.class);
		job3_2.setOutputFormatClass(TextOutputFormat.class);
			
		FileInputFormat.setInputPaths(job3_2, new Path("mystery_data/word_count/"));
		FileOutputFormat.setOutputPath(job3_2, new Path("mystery_data/author_names/"));
			
		job3_2.waitForCompletion(true);
		
		// 	Number of authors using a word
		
		Job job4=Job.getInstance(conf);
		job4.setJarByClass(MainClass.class);
		
		job4.setMapperClass(AuthorWordUseCountMapper.class);
		job4.setReducerClass(AuthorWordUseCountReducer.class);
		
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job4, new Path("mystery_data/word_count/"));
		FileOutputFormat.setOutputPath(job4, new Path("mystery_data/author_word_use_count/"));
				
		job4.waitForCompletion(true);
		
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
		MultipleInputs.addInputPath(job6, new Path("mystery_data/tf/"), TextInputFormat.class);
		FileOutputFormat.setOutputPath(job6, new Path("mystery_data/aav/"));
		
		job6.waitForCompletion(true);
		
		//	 Compute cosine similarity numerators (products only)

		Job job9=Job.getInstance(conf);
		job9.setJarByClass(MainClass.class);
		
		job9.setMapperClass(CosSimilarityNumerMapper.class);
		job9.setCombinerClass(CosSimilarityNumerCombiner.class);
		job9.setReducerClass(CosSimilarityNumerReducer.class);
		
		job9.setOutputKeyClass(Text.class);
		job9.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job9, new Path("data/aavs/"), TextInputFormat.class);
		MultipleInputs.addInputPath(job9, new Path("mystery_data/aav/"), TextInputFormat.class); 
		FileOutputFormat.setOutputPath(job9, new Path("mystery_data/cos_sim_numerator_components/"));
		
		job9.waitForCompletion(true);
		
		//	 Compute cosine similarity numerators (sum of products)

		Job job10=Job.getInstance(conf);
		job10.setJarByClass(MainClass.class);
		
		job10.setMapperClass(CosSimilarityNumerSumMapper.class);
		job10.setReducerClass(CosSimilarityNumerSumReducer.class);
		
		job10.setOutputKeyClass(Text.class);
		job10.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job10, new Path("mystery_data/cos_sim_numerator_components/"));
		FileOutputFormat.setOutputPath(job10, new Path("mystery_data/cos_sim_numerator_sums/"));
		
		job10.waitForCompletion(true);

		
		//	 Compute cosine similarity denominators (roots of squared sums)

		Job job11=Job.getInstance(conf);
		job11.setJarByClass(MainClass.class);
		
		job11.setMapperClass(CosSimilarityDenomMapper.class);
		job11.setCombinerClass(CosSimilarityDenomCombiner.class);
		job11.setReducerClass(CosSimilarityDenomReducer.class);
		
		job11.setOutputKeyClass(Text.class);
		job11.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job11, new Path("data/aavs/"), TextInputFormat.class);
		MultipleInputs.addInputPath(job11, new Path("mystery_data/aav/"), TextInputFormat.class); 
		FileOutputFormat.setOutputPath(job11, new Path("mystery_data/cos_sim_denominators/"));
		
		job11.waitForCompletion(true);
	}
}