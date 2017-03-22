package offline;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CalculateIDFReducer extends Reducer<Text,Text,Text,FloatWritable> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		
		int author_count = 0;
		if (key.toString().equals("authors:")){
			author_count++;
		}
		else {
			for (Text val: values) {
				float idf = author_count / Float.parseFloat(val.toString());
				context.write(new Text("idf_" + key.toString()), new FloatWritable(Float.parseFloat(val.toString())));
			}
		}
	}
}
