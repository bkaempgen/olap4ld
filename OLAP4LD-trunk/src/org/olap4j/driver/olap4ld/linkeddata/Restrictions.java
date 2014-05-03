package org.olap4j.driver.olap4ld.linkeddata;

import java.util.Set;

import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.metadata.Member;
import org.semanticweb.yars.nx.Node;

/**
 * Restrictions describe filters for multidimensional elements in terms of
 * Linked Data elements (Literal values and URIs).
 * 
 * @author benedikt
 * 
 */
public class Restrictions {
	// Restrictions
	public Node catalog = null;
	public Node schemaPattern = null;
	public Node cubeNamePattern = null;
	public Node dimensionUniqueName = null;
	public Node hierarchyUniqueName = null;
	public Node levelUniqueName = null;
	public Node memberUniqueName = null;
	public Integer tree = null;
	public Set<Member.TreeOp> treeOps = null;

	/**
	 * Creates empty restrictions.
	 */
	public Restrictions() {
		;
	}

	public String toString() {
		return "(catalog = " + catalog + " schemaPattern = " + schemaPattern
				+ " cubeNamePattern = " + cubeNamePattern
				+ " dimensionUniqueName = " + dimensionUniqueName
				+ " hierarchyUniqueName = " + hierarchyUniqueName
				+ " levelUniqueName = " + levelUniqueName
				+ " memberUniqueName = " + memberUniqueName + " treeOps = "
				+ tree+")";
	}

	/**
	 * Restrictions are URI representations of olap metadata objects. This
	 * constructor creates restrictions from an olap4j restrictions array.
	 * 
	 * @param restrictions
	 */
	public Restrictions(Object[] restrictions) {

		for (int i = 0; i < restrictions.length; i = i + 2) {
			if ("CATALOG_NAME".equals((String) restrictions[i])) {
				catalog = Olap4ldLinkedDataUtil
						.convertMDXtoURI((String) restrictions[i + 1]);
				// we do not consider catalogs for now.
				continue;
			}
			if ("SCHEMA_NAME".equals((String) restrictions[i])) {
				schemaPattern = Olap4ldLinkedDataUtil
						.convertMDXtoURI((String) restrictions[i + 1]);
				// we do not consider schema for now
				continue;
			}
			if ("CUBE_NAME".equals((String) restrictions[i])) {
				cubeNamePattern = Olap4ldLinkedDataUtil
						.convertMDXtoURI((String) restrictions[i + 1]);
				continue;
			}
			if ("DIMENSION_UNIQUE_NAME".equals((String) restrictions[i])) {
				dimensionUniqueName = Olap4ldLinkedDataUtil
						.convertMDXtoURI((String) restrictions[i + 1]);
				continue;
			}
			if ("HIERARCHY_UNIQUE_NAME".equals((String) restrictions[i])) {
				hierarchyUniqueName = Olap4ldLinkedDataUtil
						.convertMDXtoURI((String) restrictions[i + 1]);
				continue;
			}
			if ("LEVEL_UNIQUE_NAME".equals((String) restrictions[i])) {
				levelUniqueName = Olap4ldLinkedDataUtil
						.convertMDXtoURI((String) restrictions[i + 1]);
				continue;
			}
			if ("MEMBER_UNIQUE_NAME".equals((String) restrictions[i])) {
				memberUniqueName = Olap4ldLinkedDataUtil
						.convertMDXtoURI((String) restrictions[i + 1]);
				continue;
			}
			if ("TREE_OP".equals((String) restrictions[i])) {
				// Tree OP cannot be transformed into URI representation
				tree = new Integer((String) restrictions[i + 1]);
				// treeOps erstellen wie in OpenVirtuoso
				continue;
			}
		}
	}
}