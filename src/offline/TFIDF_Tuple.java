// cs435_pa2 Assignment
// Author: Gregory Poisson
// Date: Mar 22, 2017
// Email: gpoisson@rams.colostate.edu

package offline;

import java.util.ArrayList;

public class TFIDF_Tuple {
	
	public float tf_value;
	public float idf;
	public float tfidf_value;
	public String word;
	public String author;
	
	public TFIDF_Tuple() {
		
	}
	
	public String toString() {
		return word + "," + author + "," + tf_value + "," + idf;
	}

}
