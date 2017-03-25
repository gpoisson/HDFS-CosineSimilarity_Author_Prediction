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
			if (line_split[0].substring(0, 4).equals("idf_")){
				knowns.add(line_split[0].substring(4) + "\t" + line_split[1]);
			}
			else {
				mystery.add(line_split[0] + "\t" + line_split[1]);
			}
		}
		
		for (String known: knowns){
			String term = known.split("\t")[0];
			boolean found = false;
			for (String entry: mystery){
				String[] entry_split = entry.split("\t");
				if (term.equals(entry_split[0])){
					found = true;
					context.write(new Text(entry_split[0]), new Text(entry_split[1]));
					break;
				}
			}
			if (!found){
				float tfidf = (float) (Float.parseFloat(known.split("\t")[1]) * 0.5);
				context.write(new Text("mys_" + known), new Text(tfidf + ""));
			}
		}
	}
}
