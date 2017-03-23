// cs435_pa2 Assignment
// Author: Gregory Poisson
// Date: Mar 22, 2017
// Email: gpoisson@rams.colostate.edu

package offline;

import java.util.ArrayList;

public class TFIDF_Tuple {
	
	public ArrayList<Float> tf_values;
	public ArrayList<Float> idf;
	public ArrayList<Float> tfidf_values;
	public String word;
	public ArrayList<String> authors;
	
	public TFIDF_Tuple() {
		tf_values = new ArrayList<Float>();
		idf = new ArrayList<Float>();
		tfidf_values = new ArrayList<Float>();
		authors = new ArrayList<String>();
	}

}
