package offline;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CalculateIDFReducer extends Reducer<Text,Text,Text,FloatWritable> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		
		for (Text val: values) {
			context.write(new Text("idf_ " + key.toString()), new FloatWritable(Float.parseFloat(val.toString())));
		}
	}
}
