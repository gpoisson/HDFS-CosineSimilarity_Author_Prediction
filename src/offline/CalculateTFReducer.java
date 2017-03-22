package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CalculateTFReducer extends Reducer<Text,Text,Text,FloatWritable> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		
		// Keys: authors		Values: (<word>  <count>)
		int max_occurrances = -1;
		String most_common = new String();
		ArrayList<String> terms = new ArrayList<String>();
		ArrayList<Integer> counts = new ArrayList<Integer>();
		for (Text val: values) {
			String[] entry = val.toString().split("\t");
			assert(entry.length == 2);
			if (Integer.parseInt(entry[1]) > max_occurrances){
				max_occurrances = Integer.parseInt(entry[1]);
				most_common = entry[0];
			}
			terms.add(entry[0]);
			counts.add(Integer.parseInt(entry[1]));
		}
		
		context.write(new Text(most_common), new FloatWritable());

		//ArrayList<Float> tfs = new ArrayList<Float>();
		//ArrayList<Text> words = new ArrayList<Text>();
		
		for (int i = 0; i < terms.size(); i++) {
			int count = counts.get(i);
			float tf = (float) (0.5 + 0.5 * (((float) count) / max_occurrances));
			context.write(new Text(key.toString() + " " + terms.get(i)), new FloatWritable (tf));
		}
				
		//for (int i = 0; i < tfs.size(); i++) {
		//	context.write((words.get(i)), new FloatWritable(tfs.get(i)));
		//}
	}
}
