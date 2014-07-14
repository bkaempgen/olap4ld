package org.olap4j.driver.olap4ld.test;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;

import junit.framework.TestCase;

import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;
import org.olap4j.driver.olap4ld.linkeddata.EmbeddedSesameEngine;
import org.olap4j.driver.olap4ld.linkeddata.LogicalOlapOp;
import org.olap4j.driver.olap4ld.linkeddata.ReconciliationCorrespondence;

public class GeneralHelpFunctionsTest extends TestCase {
	
	public GeneralHelpFunctionsTest() throws SQLException {

		Olap4ldUtil.prepareLogging();
		// Logging
		// For debugging purposes
		Olap4ldUtil._log.setLevel(Level.CONFIG);

		// For monitoring usage
		//Olap4ldUtil._log.setLevel(Level.INFO);

		// For warnings (and errors) only
		// Olap4ldUtil._log.setLevel(Level.WARNING);
	}

	protected void setUp() throws SQLException {

	}

	protected void tearDown() throws Exception {

	}

	public void testEstatwrap() throws MalformedURLException {
		URL gdppercapita = new URL("http://estatwrap.ontologycentral.com/id/nama_aux_gph#ds");
		URL result = Olap4ldLinkedDataUtil.askForLocation(gdppercapita);
		assertEquals("http://estatwrap.ontologycentral.com/data/nama_aux_gph",result.toString());
	}
	
	public void testDataFu() throws MalformedURLException {
		
		URL gdppercapita = new URL("http://localhost:8080/Data-Fu-Engine/data-fu/gdp_per_capita_experiment/triples");
		URL result = Olap4ldLinkedDataUtil.askForLocation(gdppercapita);
		assertEquals("http://localhost:8080/Data-Fu-Engine/data-fu/gdp_per_capita_experiment/triples",result.toString());
	}
	
	public void testOlap4ldLinkedDataUtilnumberForChildren() {
		
		int no = Olap4ldLinkedDataUtil.numberForChildren(0, 3, 2);
		assertEquals(3, no);
		
		no = Olap4ldLinkedDataUtil.numberForChildren(1, 3, 2);
		assertEquals(18, no);
		
		no = Olap4ldLinkedDataUtil.numberForChildren(2, 3, 2);
		assertEquals(864, no);
		
		no = Olap4ldLinkedDataUtil.numberForChildren(3, 3, 2);
		assertEquals(1565568, no);
	}
	
	public void testOlap4ldLinkedDataUtilgenerateLqpsForDepth() {
		
		List<ReconciliationCorrespondence> correspondences = EmbeddedSesameEngine.getReconciliationCorrespondences();
		String[] datasets = {"http://estatwrap.ontologycentral.com/id/tec00115#ds", "http://lod.gesis.org/lodpilot/ALLBUS/ZA4570v590.rdf#ds", "http://estatwrap.ontologycentral.com/id/nama_aux_gph#ds"};
		
		List<LogicalOlapOp> lqps = Olap4ldLinkedDataUtil.generateLqpsForDepth(0, datasets, correspondences);
		assertEquals(3, lqps.size());
		
		lqps = Olap4ldLinkedDataUtil.generateLqpsForDepth(1, datasets, correspondences);
		assertEquals(18, lqps.size());
		
		lqps = Olap4ldLinkedDataUtil.generateLqpsForDepth(2, datasets, correspondences);
		assertEquals(864, lqps.size());
		
		lqps = Olap4ldLinkedDataUtil.generateLqpsForDepth(3, datasets, correspondences);
		assertEquals(1565568, lqps.size());
	}
}
