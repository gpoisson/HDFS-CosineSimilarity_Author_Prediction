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

import profile_1A.WordTuple;

public class MainClass {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		if (args.length != 2) {
			System.out.printf("Usage: <jar file> <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		
		Job job=Job.getInstance(conf);
		job.setJarByClass(MainClass.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		ArrayList<WordTuple> globalWordCounts = new ArrayList<WordTuple>();
		
		String textFile = "part-r-00000";
		BufferedReader reader;
		
		reader = new BufferedReader(new FileReader(textFile));
		
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}