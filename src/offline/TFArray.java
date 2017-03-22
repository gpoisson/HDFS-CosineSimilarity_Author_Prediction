package offline;

import java.util.ArrayList;

public class TFArray {
	
	public WordTuple most_common;
	public ArrayList<WordTuple> words;
	public ArrayList<Float> tf_array;
	
	public TFArray(WordTuple most_common, ArrayList<WordTuple> words) {
		this.most_common = most_common;
		this.words = words;
		this.tf_array = new ArrayList<Float>();
	}
	

	//	TF = 0.5 + 0.5(fij / maxk fkj)
	public void compute() {
		for (WordTuple word: words) {
			float tf = (float) (0.5 + 0.5 * (word.count / most_common.count));
			tf_array.add(tf);
		}
	}

}
