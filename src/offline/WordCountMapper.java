package offline;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	// profile 1-A
	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		String[] lines = value.toString().split("\n");
		for (String line: lines) {
			line = line.toLowerCase();
			line = line.replaceAll("[!@#$%^&*()_+~`\"\\{}:;',.?/|]", "");
			
			String[] line_split = line.split("<===>");
			// line_split = <author>  <date>  <text>
			
			String date = line_split[1];
			// date = <month day year>
			String[] date_split = date.split(" ");
			// date_split = <month>  <day>  <year>
			String year = date_split[date_split.length-1];
			
			String[] words = line_split[line_split.length-1].split(" ");
			// words = <word1>  <word2>  ....  <wordN>
			
			for (String word: words){
				String out = word + "\t" + year;
				context.write(new Text(out), new Text("one"));
			}
		}
	}
}