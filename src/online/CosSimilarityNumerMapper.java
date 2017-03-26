package online;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CosSimilarityNumerMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		String[] lines = value.toString().split("\n");

		for (String line: lines) {
			String[] line_split = line.split("\t");
			String term = line_split[1];
			String author = line_split[0];
			String tfidf = line_split[2];
			context.write(new Text(author), new Text(tfidf + "\t" + term));
		}
	}
}
