package org.olap4j.driver.ld;

import java.util.List;

import org.olap4j.Position;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;

public class LdOlapQuery {

	public LdOlap4jCube cube;
	public List<Level> slicesrollups;
	public List<Position> dices;
	public List<Measure> projections;

	public LdOlapQuery(LdOlap4jCube cube, List<Level> slicesrollups,
			List<Position> dices,
			List<Measure> projections) {
		this.cube = cube;
		this.slicesrollups = slicesrollups;
		this.dices = dices;
		this.projections = projections;
	}
	
}
