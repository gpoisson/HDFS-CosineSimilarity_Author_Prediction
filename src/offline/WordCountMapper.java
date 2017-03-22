package offline;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	// profile 1-A
	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		String[] lines = value.toString().split("\n");
		for (String line: lines) {
			line = line.toLowerCase();
			
			String[] line_split = line.split("<===>");
			// line_split = <author>  <date>  <text>
			
			String author = line_split[0];
			
			String date = line_split[1];
			// date = <month day year>
			String[] date_split = date.split(" ");
			// date_split = <month>  <day>  <year>
			String year = date_split[date_split.length-1];
			
			String cleaned_text = new String();
			ArrayList<String> words = new ArrayList<String>();
			for (int i = 0; i < line_split[line_split.length-1].length(); i++) {
				if (line_split[line_split.length-1].charAt(i) >= 'a' && line_split[line_split.length-1].charAt(i) <= 'z') {
					cleaned_text += line_split[line_split.length-1].charAt(i);
				}
				else if ((line_split[line_split.length-1].charAt(i) == ' ') || (line_split[line_split.length-1].charAt(i) == '\t') || (line_split[line_split.length-1].charAt(i) == '\n')) {
					if (cleaned_text.length() > 0) {
						words.add(cleaned_text);
						cleaned_text = new String();
					}
				}
			}
			
			
			// words = <word1>  <word2>  ....  <wordN>
			
			for (String word: words){
				String out = word + "\t" + author;
				context.write(new Text(out), new Text("one"));
			}
		}
	}
}