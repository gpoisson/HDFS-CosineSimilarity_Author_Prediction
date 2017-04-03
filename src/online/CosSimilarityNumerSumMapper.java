package online;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CosSimilarityNumerSumMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		String[] lines = value.toString().split("\n");

		for (String line: lines) {
			String[] split = line.split("\t");
			String author = split[0];
			String term = split[1];
			double tfidf = Double.parseDouble(split[2]);
			
			context.write(new Text(author), new Text(term + "\t" + tfidf));
		}
	}
}
