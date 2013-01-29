package org.olap4j.driver.ld.test;

import org.olap4j.driver.ld.HumaniseCamelCase;

import junit.framework.TestCase;

public class HumaniseCamelCaseTest extends TestCase {

	public void testHumanise() {
		HumaniseCamelCase myCamel = new HumaniseCamelCase();
		String numberHumanise = myCamel.humanise("dtendFirst2010-06-30");
		System.out.println(numberHumanise);
		assertEquals(numberHumanise, "Dtend first 2010-06-30");
	}

}