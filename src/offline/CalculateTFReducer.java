package offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Compute Term Frequency
public class CalculateTFReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		
		int max_occurrances = -1;
		ArrayList<String> terms = new ArrayList<String>();
		ArrayList<Integer> counts = new ArrayList<Integer>();
		for (Text val: values) {
			String[] entry = val.toString().split("\t");
			assert(entry.length == 2);
			if (Integer.parseInt(entry[1]) > max_occurrances){
				max_occurrances = Integer.parseInt(entry[1]);
			}
			terms.add(entry[0]);
			counts.add(Integer.parseInt(entry[1]));
		}
		
		for (int i = 0; i < terms.size(); i++) {
			int count = counts.get(i);
			float tf = (float) (0.5 + 0.5 * (((float) count) / max_occurrances));
			context.write(new Text("tf_" + key.toString() + "\t" + terms.get(i)), new Text (tf + ""));
		}
	}
}
