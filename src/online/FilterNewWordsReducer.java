package online;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Find new words used by mystery author and remove them
public class FilterNewWordsReducer  extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		
		ArrayList<String> knowns = new ArrayList<String>();
		ArrayList<String> mystery = new ArrayList<String>();
		
		for (Text val: values) {
			
			String[] line_split = val.toString().split("\t");
			
			String term = key.toString();
			String value = line_split[0];
			String author = line_split[1];
			
			if (author.equals("idf_")){
				knowns.add(term + "\t" + value + "\t" + author);
			}
			else {
				mystery.add(term + "\t" + value + "\t" + author);
			}
		}
		
		for (String author_term_tfidf: mystery){
			String term = author_term_tfidf.split("\t")[0];
			String tfidf = author_term_tfidf.split("\t")[1];
			String author = author_term_tfidf.split("\t")[2];
			for (String term_idf_author: knowns){
				String known_term = term_idf_author.split("\t")[0];
				if (term.equals(known_term)){
					context.write(new Text(term), new Text(author + "\t" + tfidf));
					break;
				}
			}
		}
		
	}
}
