package offline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AuthorWordUseCountReducer extends Reducer<Text,Text,Text,IntWritable> {

	public void reduce(Text  key,  Iterable<Text>  values,  Context  context) throws IOException, InterruptedException {
		int count = 0;
		
		for (Text val: values) {
			count++;
		}
		if (key != null && count > 0) {
			context.write(key, new IntWritable(count));
		}
	}
}
