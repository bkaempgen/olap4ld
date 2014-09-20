package org.olap4j.driver.olap4ld.test;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.logging.Level;

import junit.framework.TestCase;

import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.olap4j.driver.olap4ld.helper.Olap4ldLinkedDataUtil;

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
}
