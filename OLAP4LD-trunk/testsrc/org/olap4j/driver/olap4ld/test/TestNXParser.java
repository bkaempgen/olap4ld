package org.olap4j.driver.olap4ld.test;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.NxParser;

import junit.framework.TestCase;


public class TestNXParser extends TestCase {
	public void testParseNX() throws Exception {
		String myqcrumburl = "http://qcrumb.com/sparql";
		String querysuffix = "?query=" + URLEncoder.encode("select * FROM <http://estatwrap.ontologycentral.com/dic/geo> where {?s ?p ?o} limit 10", "UTF-8");
		String rulesuffix = "&rules=";
		String acceptsuffix = "&accept=text%2Fnx";

		String fullurl = myqcrumburl + querysuffix + rulesuffix + acceptsuffix;
		// String fullurl =
		// "http://qcrumb.com/sparql?query=PREFIX+sdmx-measure%3A+<http%3A%2F%2Fpurl.org%2Flinked-data%2Fsdmx%2F2009%2Fmeasure%23>%0D%0APREFIX+dcterms%3A+<http%3A%2F%2Fpurl.org%2Fdc%2Fterms%2F>%0D%0APREFIX+eus%3A+<http%3A%2F%2Fontologycentral.com%2F2009%2F01%2Feurostat%2Fns%23>%0D%0APREFIX+rdf%3A+<http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23>%0D%0APREFIX+qb%3A+<http%3A%2F%2Fpurl.org%2Flinked-data%2Fcube%23>%0D%0APREFIX+rdfs%3A+<http%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23>%0D%0A%0D%0ASELECT+%3Ftime+%3Fvalue+%3Fgeo%0D%0AFROM+<http%3A%2F%2Festatwrap.ontologycentral.com%2Fdata%2Ftsieb020>%0D%0AFROM+<http%3A%2F%2Festatwrap.ontologycentral.com%2Fdic%2Fgeo>%0D%0AWHERE+{%0D%0A++%3Fs+qb%3Adataset+<http%3A%2F%2Festatwrap.ontologycentral.com%2Fid%2Ftsieb020%23ds>+.%0D%0A++%3Fs+dcterms%3Adate+%3Ftime+.%0D%0A++%3Fs+eus%3Ageo+%3Fg+.%0D%0A++%3Fg+rdfs%3Alabel+%3Fgeo+.%0D%0A++%3Fs+sdmx-measure%3AobsValue+%3Fvalue+.%0D%0A++FILTER+(lang(%3Fgeo)+%3D+\"en\")%0D%0A}+ORDER+BY+%3Fgeo%0D%0A&rules=&accept=application%2Fsparql-results%2Bjson"

		HttpURLConnection con = (HttpURLConnection) new URL(fullurl)
				.openConnection();
		// TODO: How to properly set header? Does not work therefore manually
		// added
		con.setRequestProperty("Accept", "application/sparql-results+json");
		con.setRequestMethod("POST");

		if (con.getResponseCode() != 200) {
			throw new RuntimeException("lookup on " + fullurl
					+ " resulted HTTP in status code " + con.getResponseCode());
		}

		System.out.println(fullurl);
		
		//String test = convertStreamToString(con.getInputStream());

		NxParser nxp = new NxParser(con.getInputStream());

		Node[] nxx;
		while (nxp.hasNext()) {
			nxx = nxp.next();
			System.out.println(Nodes.toN3(nxx));
		} 
	}
}
