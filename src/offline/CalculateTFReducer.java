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
		int max_occurrances = 0;
		String most_common = new String();
		for (Text val: values) {
			String[] entry = val.toString().split("\t");
			if (Integer.parseInt(entry[1]) > max_occurrances){
				max_occurrances = Integer.parseInt(entry[1]);
				most_common = entry[0];
			}
		}

		ArrayList<Float> tfs = new ArrayList<Float>();
		ArrayList<Text> words = new ArrayList<Text>();
		
		for (Text val: values) {
			String[] entry = val.toString().split("\t");
			String term = entry[0];
			int count = Integer.parseInt(entry[1]);
			float tf = (float) (0.5 + 0.5 * (((float) count) / max_occurrances));
			tfs.add(tf);
			words.add(new Text(key.toString() + " " + val.toString()));
		}
		
		for (int i = 0; i < tfs.size(); i++) {
			context.write((words.get(i)), new FloatWritable(tfs.get(i)));
		}
	}
}
