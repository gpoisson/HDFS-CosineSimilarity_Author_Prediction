package online;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AdjustDimensionsReducer  extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {

		ArrayList<String> knowns = new ArrayList<String>();
		ArrayList<String> mystery = new ArrayList<String>();
		
		for (Text val: values) {
			String[] line_split = val.toString().split("\t");
			if (key.toString().substring(0, 4).equals("idf_")){
				knowns.add(key.toString().substring(4));
				context.write(new Text("KEY: " + key), new Text("VAL (known): " + val));
			}
			else {
				mystery.add(key.toString() + "\t" + line_split[0]);
				context.write(new Text("KEY: " + key), new Text("VAL (mystery): " + val));
			}
		}
		
		for (String known: knowns){
			String author = null;
			String term = null;
			String tfidf = null;
			boolean found = false;
			for (String entry: mystery){
				author = entry.split("\t")[0];
				term = entry.split("\t")[1];
				tfidf = entry.split("\t")[2];
				if (term.equals(known)){
					found = true;
					//context.write(new Text(author + "\t" + term), new Text(tfidf));
					break;
				}
			}
			if (!found){
				
				//context.write(new Text(author + "\t" + term), new Text(tfidf));
			}
		}
	}
}
