package org.olap4j.driver.olap4ld.helper;

import java.util.Set;

import org.olap4j.metadata.Member;

public class Restrictions {
	// Restrictions
	public String catalog = null;
	public String schemaPattern = null;
	public String cubeNamePattern = null;
	public String dimensionUniqueName = null;
	public String hierarchyUniqueName = null;
	public String levelUniqueName = null;
	public String memberUniqueName = null;
	public Integer tree = null;
	public Set<Member.TreeOp> treeOps = null;

	public Restrictions(Object[] restrictions) {

		for (int i = 0; i < restrictions.length; i = i + 2) {
			if ("CATALOG_NAME".equals((String) restrictions[i])) {
				catalog = (String) restrictions[i + 1];
				// we do not consider catalogs for now.
				continue;
			}
			if ("SCHEMA_NAME".equals((String) restrictions[i])) {
				schemaPattern = (String) restrictions[i + 1];
				// we do not consider schema for now
				continue;
			}
			if ("CUBE_NAME".equals((String) restrictions[i])) {
				cubeNamePattern = (String) restrictions[i + 1];
				continue;
			}
			if ("DIMENSION_UNIQUE_NAME".equals((String) restrictions[i])) {
				dimensionUniqueName = (String) restrictions[i + 1];
				continue;
			}
			if ("HIERARCHY_UNIQUE_NAME".equals((String) restrictions[i])) {
				hierarchyUniqueName = (String) restrictions[i + 1];
				continue;
			}
			if ("LEVEL_UNIQUE_NAME".equals((String) restrictions[i])) {
				levelUniqueName = (String) restrictions[i + 1];
				continue;
			}
			if ("MEMBER_UNIQUE_NAME".equals((String) restrictions[i])) {
				memberUniqueName = (String) restrictions[i + 1];
				continue;
			}
			if ("TREE_OP".equals((String) restrictions[i])) {
				tree = new Integer((String) restrictions[i + 1]);
				// treeOps erstellen wie in OpenVirtuoso
				continue;
			}
		}
	}
}