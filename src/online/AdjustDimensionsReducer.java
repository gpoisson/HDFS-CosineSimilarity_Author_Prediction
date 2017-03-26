package online;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Add unused terms to Author Attribute Vector to make its dimension match the known AAVs
public class AdjustDimensionsReducer  extends Reducer<Text,Text,Text,Text>{
	
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
		
		for (String term_idf_type: knowns){
			boolean found = false;
			String term = term_idf_type.split("\t")[0];
			for (String term_tfidf_author: mystery){
				String mystery_term = term_tfidf_author.split("\t")[0];
				String tfidf = term_tfidf_author.split("\t")[1];
				if (mystery_term.equals(term)){
					found = true;
					context.write(new Text("unknown author"), new Text(term + "\t" + tfidf));
					break;
				}
			}
			if (!found){
				float idf = Float.parseFloat(term_idf_type.split("\t")[1]);
				float tfidf = (float) (idf * 0.5);
				context.write(new Text("unknown author"), new Text(term + "\t" + tfidf));
			}
		}
	}
}
