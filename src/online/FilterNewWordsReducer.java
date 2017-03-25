package online;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterNewWordsReducer  extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		
		ArrayList<String> knowns = new ArrayList<String>();
		ArrayList<String> mystery = new ArrayList<String>();
		
		for (Text val: values) {
			String[] line_split = val.toString().split("\t");
			if (line_split[0].substring(0, 4).equals("idf_")){
				knowns.add(line_split[0].substring(4));
			}
			else {
				mystery.add(line_split[0] + "\t" + line_split[1]);
			}
		}
		
		for (String entry: mystery){
			String term = entry.split("\t")[0];
			String tfidf = entry.split("\t")[1];
			boolean found = false;
			for (String known: knowns){
				if (known.equals(term)){
					found = true;
					break;
				}
			}
			if (found){
				context.write(new Text(term), new Text(tfidf));
			}
		}
	}
}
