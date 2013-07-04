package org.olap4j.driver.olap4ld;

import java.util.List;

import org.olap4j.Position;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;

@Deprecated
public class Olap4ldQuery {

	public Olap4ldCube cube;
	public List<Level> slicesrollups;
	public List<Position> dices;
	public List<Measure> projections;

	public Olap4ldQuery(Olap4ldCube cube, List<Level> slicesrollups,
			List<Position> dices,
			List<Measure> projections) {
		this.cube = cube;
		this.slicesrollups = slicesrollups;
		this.dices = dices;
		this.projections = projections;
	}
	
}
