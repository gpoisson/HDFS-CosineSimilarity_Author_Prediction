package online;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Find new words used by mystery author and remove them
public class FilterNewWordsMapper  extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		String[] lines = value.toString().split("\n");
		ArrayList<String> knowns = new ArrayList<String>();
		ArrayList<String> mystery = new ArrayList<String>();
		for (String line: lines) {
			
			String[] line_split = line.split("\t");
			
			if (line_split.length == 2){
				// Known terms
				String term = line_split[0].substring(4);
				String idf = line_split[1];
				knowns.add(term + "\t" + idf);
				context.write(new Text(term), new Text(idf + "\tidf_"));
			}
			if (line_split.length == 3){
				// Mystery author terms
				String author = line_split[0];
				String term = line_split[1];
				String tfidf = line_split[2];
				mystery.add(author + "\t" + term + "\t" + tfidf);
				context.write(new Text(term), new Text(tfidf + "\t" + author));
			}
			
		}
	}
}
