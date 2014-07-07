package org.olap4j.driver.olap4ld.linkeddata;

import java.util.List;
import org.semanticweb.yars.nx.Node;

/**
 * A reconciliation correspondence declaratively describes a relationship
 * between multidimensional datasets.
 * 
 * It consists of a convert cube function name and a set of triple patterns
 * denoting the inputmembers1, inputmember2... as well as the outputmembers.
 * 
 * @author benedikt
 * 
 */
public class ReconciliationCorrespondence {

	List<Node[]> inputmembers1;
	List<Node[]> inputmembers2;
	List<Node[]> outputmembers;
	private String conversionName;
	private String function;

	public ReconciliationCorrespondence(String conversionName,
			List<Node[]> inputmembers1, List<Node[]> inputmembers2,
			List<Node[]> outputmembers, String function) {
		this.conversionName = conversionName;
		this.inputmembers1 = inputmembers1;
		this.inputmembers2 = inputmembers2;
		this.outputmembers = outputmembers;
		this.function = function;
	}

	public String getname() {
		return conversionName;
	}

	public List<Node[]> getInputmembers1() {
		return inputmembers1;
	}

	public List<Node[]> getInputmembers2() {
		return inputmembers2;
	}

	public List<Node[]> getOutputmembers() {
		return outputmembers;
	}

	public String getFunction() {
		return function;
	}

	public String toString() {
		String reconciliationcorrespondenceString = "(" + conversionName
				+ ", {";

		for (int i = 0; i < inputmembers1.size(); i++) {
			reconciliationcorrespondenceString += "(" + inputmembers1.get(i)[0]
					+ "," + inputmembers1.get(i)[1] + ")";
		}

		reconciliationcorrespondenceString += "},{";

		if (inputmembers2 != null) {
			for (int i = 0; i < inputmembers2.size(); i++) {
				reconciliationcorrespondenceString += "("
						+ inputmembers2.get(i)[0] + ","
						+ inputmembers2.get(i)[1] + ")";
			}
		}
		
		reconciliationcorrespondenceString += "},{";
		
		if (outputmembers != null) {
			for (int i = 0; i < outputmembers.size(); i++) {
				reconciliationcorrespondenceString += "("
						+ outputmembers.get(i)[0] + ","
						+ outputmembers.get(i)[1] + ")";
			}
		}
		
		reconciliationcorrespondenceString += "},{";
		
		reconciliationcorrespondenceString += function;
		
		reconciliationcorrespondenceString += "})";

		return reconciliationcorrespondenceString;
	}
}
