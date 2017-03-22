package offline;

import java.util.ArrayList;

public class IDFArray {

	ArrayList<Float> idf_array;
	ArrayList<WordTuple> words;
	GlobalCount gc;
	int N;
	
	/*
	 * 			IDFi = log (N / ni)
	 *	 			N: Number of authors
	 * 				ni: Number of authors who use the term <i>
	 */
	public IDFArray(int N, ArrayList<WordTuple> words, GlobalCount gc) {
		idf_array = new ArrayList<Float>();
		this.words = words;
		this.gc = gc;
		this.N = N;
	}
	
	public void compute() {
		for (int i = 1; i < words.size(); i++) {
			float idf = (float) Math.log(N / gc.number_of_authors_who_use_term(words.get(i).word));
			idf_array.add(idf);
		}
	}

}
