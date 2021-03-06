package offline;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Count the number of occurrences of a given word
public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable   key,   Text   value,   Context   context) throws IOException, InterruptedException{
		String[] lines = value.toString().split("\n");
		for (String line: lines) {
			line = line.toLowerCase();
			
			String[] line_split = line.split("<===>");
			
			String author = line_split[0];
			
			String raw_text = line_split[line_split.length-1];
			String cleaned_text = new String();
			ArrayList<String> words = new ArrayList<String>();
			for (int i = 0; i < raw_text.length(); i++) {
				if (raw_text.charAt(i) >= 'a' && raw_text.charAt(i) <= 'z') {
					cleaned_text += line_split[line_split.length-1].charAt(i);
				}
				else if ((raw_text.charAt(i) != ' ') && (raw_text.charAt(i) != '\t')) {
					
				}
				else if (cleaned_text.length() > 0) {
					words.add(cleaned_text);
					cleaned_text = new String();
				}
			}
			if (cleaned_text.length() > 0) {
				words.add(cleaned_text);
				cleaned_text = new String();
			}
			
			
			for (String word: words){
				String out = word + "\t" + author;
				context.write(new Text(out), new Text("one"));
			}
		}
	}
}