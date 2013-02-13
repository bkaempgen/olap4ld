package org.olap4j.driver.ld;

import java.util.ArrayList;
import java.util.List;

import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;

public class LdOlapQuery {

	public LdOlap4jCube cube;
	public ArrayList<Level> slicesrollups;
	public ArrayList<List<Member>> dices;
	public ArrayList<Measure> projections;

	public LdOlapQuery(LdOlap4jCube cube, ArrayList<Level> slicesrollups,
			ArrayList<List<Member>> dices,
			ArrayList<Measure> projections) {
		this.cube = cube;
		this.slicesrollups = slicesrollups;
		this.dices = dices;
		this.projections = projections;
	}
	
}
