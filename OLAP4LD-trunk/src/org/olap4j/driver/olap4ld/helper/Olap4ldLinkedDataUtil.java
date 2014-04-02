package org.olap4j.driver.olap4ld.helper;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.olap4j.driver.olap4ld.Olap4ldUtil;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;

public class Olap4ldLinkedDataUtil {

	/*
	 * RDF prefixes
	 */
	static HashMap<Integer, String> standard_prefix2uri = null;
	static HashMap<Integer, String> standard_uri2prefix = null;

	/*
	 * As stated in encodeNode2Mdx
	 */
	public static final String MEASURE_DIMENSION_NAME = "Measures";

	/**
	 * Elements (identifiers) used in MDX need to follow certain rules in order
	 * to be parseable from and to MDX. In fact, having it URL encoded is not
	 * enough, also, % and . need to be replaced.
	 * 
	 * @param baseuri
	 *            The pure uri string to be encoded for MDX
	 * @return encoded string
	 */
	static String encodeUriWithPrefix(String uri) {

		// Since we do not manage prefixes, we have to come up with one
		// ourselves
		// (which if possible should be always the same):
		return encodeSpecialMdxCharactersInNames(uri);
	}
	
	/**
	 * Helper Method for asking for location
	 * 
	 * @param uri
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public static URL askForLocation(URL uri) throws MalformedURLException {

		Olap4ldUtil._log.config("Ask for location: " + uri + "...");

		String returnurlstring = null;

		HttpURLConnection.setFollowRedirects(false);
		HttpURLConnection connection;
		try {
			connection = (HttpURLConnection) uri.openConnection();
			connection.setConnectTimeout(5000);
			//int responsecode = connection.getResponseCode();
			connection.setRequestProperty("Accept", "application/rdf+xml");
			String header = connection.getHeaderField("location");
			String domain = uri.getHost();
			String protocol = uri.getProtocol();
			String port = "";
			if (uri.getPort() != 80 && uri.getPort() != -1) {
				port = ":"+uri.getPort()+"";
			}
			String path = uri.getPath();
			String query = uri.getQuery();
			if (query == null || query.equals("")) {
				query = "";
			} else {
				query = "?"+query;
			}

			
			// TODO: Could be that we need to check whether bogus comes out
			// (e.g., Not found).
			if (header != null) {

				// Header may be a absolute or relative URL
				if (header.startsWith("http:") || header.startsWith("https:")) {
					// absolute URL
					returnurlstring = header;
				} else if (header.startsWith("/")) {
					returnurlstring = protocol + "://" + domain + port
							+ header + query;
				} else {
					/*
					 * relative URL May be: Gleiche Domäne, Gleiche Ressource,
					 * ein Pfad-Segment-Aufwärts, gleiches Pfad-Segment (see
					 * http://de.wikipedia.org/wiki/Uniform_Resource_Locator#
					 * Relative_URL)
					 */

					returnurlstring = protocol + "://" + domain + port
							+ path + header + query;
				}
			} else {
				returnurlstring = protocol + "://" + domain + port
						+ path + query;
			}
			// We should remove # uris
			if (returnurlstring.contains("#")) {
				int index = returnurlstring.lastIndexOf("#");
				returnurlstring = returnurlstring.substring(0, index);
			}
		} catch (IOException e) {
			throw new MalformedURLException(e.getMessage());
		}

		Olap4ldUtil._log.config("... result: " + returnurlstring);
		return new URL(returnurlstring);
	}

	/**
	 * Decode an MDX identifier of a URI.
	 * 
	 * 
	 * @param uri
	 * @return
	 */
	static String decodeUriWithPrefix(String encodedname) {

		if (standard_prefix2uri == null && standard_uri2prefix == null) {
			readInStandardPrefixes();
		}

		String decodedname = decodeSpecialMdxCharactersInNames(encodedname);

		// In this case, the : is the sign
		int lastIndexColon = decodedname.lastIndexOf(":");

		if (lastIndexColon >= 0) {
			String qname = decodedname.substring(lastIndexColon + 1);
			// Without colon
			String prefix = decodedname.substring(0, lastIndexColon);

			String prefixuri = standard_prefix2uri.get(prefix.hashCode());

			if (prefixuri != null) {
				return prefixuri + qname;
			}
		}

		return decodedname;

	}

	/**
	 * The problem with querying Linked Data for OLAP4LD is that sometimes
	 * values returned by LinkedDataEngine need to be further processed to be
	 * usable in ResultSet and metadata objects:
	 * 
	 * * URIs used for unique names need to be translated into an MDX friendly
	 * format * Instead of null values, nx format uses "null" (TODO: or by now
	 * something else?) For certain returned values this needs to be transformed
	 * into a proper null.
	 * 
	 * This means: LinkedDataEngine should always return literal values if no
	 * encoding needs to be done. URI that it returns should be transformed for
	 * use in MDX. If LinkedDataEngine returns "null" (or similar), it is
	 * transformed into proper null. All program logic, e.g., if no CAPTION is
	 * available (so that CAPTION returns null) then use UNIQUE_NAME is either
	 * implemented by the client or LinkedDataEngine.
	 * 
	 * @param node
	 * @return
	 */
	public static String convertNodeToMDX(org.semanticweb.yars.nx.Node node) {
		// If value is uri, then convert into MDX friendly format
		if (node.toString().equals("null")) {
			// XXX: Not sure whether it is clever to have this like an object name with square br.
			// No this is complete nonsense. We need to make sure that "null" gets returned by rowset and null by metadata object.  
			return null;
		} else if (node.toString().equals("Measures")) {
			// Measures does not get encoded, can stay.
			return node.toString();
		} else {
			// No matter of uri or Literal, we need to encode it
			// We add square brackets
			return "[" + encodeUriWithPrefix(node.toString()) + "]";
		}
	}

	public static String convertMDXtoURI(String mdx) {
		if (mdx.equals("Measures")) {
			// No conversion needed.
			return mdx;
		}
		// First, we remove the square brackets
		mdx = removeSquareBrackets(mdx);

		return decodeUriWithPrefix(mdx);
	}

	public static String readInQueryTemplate(String name) {
		try {
			StreamSource stream = new StreamSource(
					Olap4ldLinkedDataUtil.class.getResourceAsStream("/" + name));
			InputStream inputStream = stream.getInputStream();

			InputStreamReader reader = new InputStreamReader(inputStream);
			BufferedReader in = new BufferedReader(reader);

			String querytemplate = "";
			String readString;
			while ((readString = in.readLine()) != null) {
				querytemplate += readString;
			}
			in.close();
			return querytemplate;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Plan to deal with prefixes is: Have to hashes, one from prexis to uri,
	 * one from uri to prefix. Then, for a given prefix/uri, we can simply
	 * return the other.
	 */
	private static void readInStandardPrefixes() {
		standard_prefix2uri = new HashMap<Integer, String>();
		standard_uri2prefix = new HashMap<Integer, String>();
		String trennzeichen = ";";
		try {
			StreamSource stream = new StreamSource(
					Olap4ldLinkedDataUtil.class
							.getResourceAsStream("/standardprefixes.csv"));
			InputStream inputStream = stream.getInputStream();
			InputStreamReader reader = new InputStreamReader(inputStream);
			BufferedReader in = new BufferedReader(reader);

			String readString;
			while ((readString = in.readLine()) != null) {
				String[] prefixuricombination = readString.split(trennzeichen);
				// Add to our prefix hashmaps
				standard_prefix2uri.put(prefixuricombination[0].hashCode(),
						prefixuricombination[1]);
				standard_uri2prefix.put(prefixuricombination[1].hashCode(),
						prefixuricombination[0]);
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * SPARQL does not allow all characters as parameter names, e.g.,
	 * ?sdmx-measure:obsValue. Therefore, we transform the URI representation
	 * into a parameter.
	 * 
	 * @param uriRepresentation
	 * @return
	 */
	public static Variable makeUriToVariable(String uriRepresentation) {
		// We simply remove all special characters
		uriRepresentation = uriRepresentation.replaceAll("[^a-zA-Z0-9]+", "");
		return new Variable("?"+uriRepresentation);
	}

	/**
	 * Regarding prefixes, we use them to store MDX compliant names for
	 * metadata. If we use those names in SPARQL queries, we either need to
	 * define them explicitly or translate the metadata into uris. We have
	 * standard prefixes that are used for encoding and decoding. It is a
	 * smaller list, therefore, it can be explicitly defined at the beginning of
	 * a SPARQL query (even though not all might be used by the query). We also
	 * store a separate list of specific prefixes that are created at run time
	 * and which do encoding/decoding, but which are not explicitly defined in a
	 * SPARQL query.
	 * 
	 * Deprecated, since we do not dynamically build SPARQL queries but use
	 * templates. Not true, since we still use it for OLAP queries.
	 * 
	 * @return
	 */
	public static String getStandardPrefixes() {
		if (standard_prefix2uri == null && standard_uri2prefix == null) {
			readInStandardPrefixes();
		}

		String standardprefixes = "";
		Collection<String> uris = standard_prefix2uri.values();
		for (String uri : uris) {
			String prefix = standard_uri2prefix.get(uri.hashCode());
			standardprefixes += "PREFIX " + prefix + ": <" + uri + "> \n";
		}

		return standardprefixes;
	}

	/**
	 * This method, either a proper caption or a uri can be given.
	 * 
	 * @param caption
	 * @return
	 */
	public static String makeCaption(String caption, String alternative) {

		if (caption == null || caption.equals("") || caption.equals("null")) {
			caption = alternative;
		}

		if (caption.contains("http://")) {

			String value = "";
			// If there is a # I take everything from there
			int hashindex = caption.indexOf('#');

			if (hashindex != -1) {
				value = caption.substring(hashindex);
			} else {
				int slashindex = caption.lastIndexOf('/');
				if (slashindex != -1) {
					value = caption.substring(slashindex);
				} else {
					value = caption;
				}
			}

			// resolve CamelCase
			HumaniseCamelCase myCamel = new HumaniseCamelCase();
			value = myCamel.humanise(value);
			return value;

		} else {
			return caption;
		}
	}

	/**
	 * Needed for parsing nx triples.
	 * 
	 * @param is
	 * @return
	 * @throws IOException
	 */
	public static String convertStreamToString(InputStream is)
			throws IOException {
		/*
		 * To convert the InputStream to String we use the Reader.read(char[]
		 * buffer) method. We iterate until the Reader return -1 which means
		 * there's no more data to read. We use the StringWriter class to
		 * produce the string.
		 */
		if (is != null) {
			Writer writer = new StringWriter();

			char[] buffer = new char[1024];
			try {
				Reader reader = new BufferedReader(new InputStreamReader(is,
						"UTF-8"));
				int n;
				while ((n = reader.read(buffer)) != -1) {
					writer.write(buffer, 0, n);
				}
			} finally {
				is.close();
			}
			return writer.toString();
		} else {
			return "";
		}
	}

	@SuppressWarnings("unused")
	@Deprecated
	private static String getSkosPropertyOfDimension(String dimensionProperty) {
		// It can only be either literal or resource: notion, or
		// exactMatch. How do I know which one? To make this query faster, we
		// need to know.
		/*
		 * TODO: For the moment, I have it hardcoded, whether skos:notation or
		 * skos:exactMatch
		 */
		String representation = null;
		if (dimensionProperty
				.equals("http://www.w3.org/2002/12/cal/ical#dtstart")) {
			representation = "skos:notation";
		} else if (dimensionProperty
				.equals("http://www.w3.org/2002/12/cal/ical#dtend")) {
			representation = "skos:notation";
		} else if (dimensionProperty.equals("http://purl.org/dc/terms/date")) {
			representation = "skos:notation";
		} else if (dimensionProperty
				.equals("http://edgarwrap.ontologycentral.com/vocab/edgar#issuer")) {
			representation = "skos:exactMatch";
		} else if (dimensionProperty
				.equals("http://edgarwrap.ontologycentral.com/vocab/edgar#segment")) {
			representation = "skos:notation";
		} else if (dimensionProperty
				.equals("http://edgarwrap.ontologycentral.com/vocab/edgar#subject")) {
			representation = "skos:exactMatch";
			// TODO: Add more.
		} else if (dimensionProperty
				.equals("http://purl.org/linked-data/cube#dataSet")) {
			representation = "skos:exactMatch";
		} else if (dimensionProperty
				.equals("http://ffiecwrap.ontologycentral.com/vocab/ffiec-concepts#RCONA001")) {
			representation = "skos:notation";
		} else if (dimensionProperty
				.equals("http://ffiecwrap.ontologycentral.com/vocab/ffiec#issuer")) {
			representation = "skos:exactMatch";
		} else if (dimensionProperty
				.equals("http://rdf.freebase.com/ns/business.business_operation.industry")) {
			representation = "skos:exactMatch";
		} else {
			representation = "?anyskos";
			// throw new UnsupportedOperationException(
			// "Olap4ld does not know this dimension property you are using.");
		}
		return representation;
	}

	/**
	 * Method to join array elements of type string
	 * 
	 * @author Hendrik Will, imwill.com
	 * @param inputArray
	 *            Array which contains strings
	 * @param glueString
	 *            String between each array element
	 * @return String containing all array elements seperated by glue string
	 */
	public static String implodeArray(String[] inputArray, String glueString) {

		/** Output variable */
		String output = "";

		if (inputArray.length > 0) {
			StringBuilder sb = new StringBuilder();
			sb.append(inputArray[0]);

			for (int i = 1; i < inputArray.length; i++) {
				sb.append(glueString);
				sb.append(inputArray[i]);
			}

			output = sb.toString();
		}

		return output;
	}

	/**
	 * Removes []
	 * 
	 * @param name
	 * @return
	 */
	private static String removeSquareBrackets(String name) {
		if (!name.startsWith("[") || !name.endsWith("]")) {
			throw new UnsupportedOperationException(
					"Name not surrounded by square brackets!");
		} else {
			return name.substring(1, name.length() - 1);
		}
	}

	/**
	 * We need to make sure that names of multidimensional elements do not carry
	 * any MDX special characters.
	 * 
	 * Note: You can use a bash command for translating a URI into an MDX
	 * applicable form:
	 * 
	 * echo "http://lod.gesis.org/lodpilot/ALLBUS/geo.rdf#list" | sed
	 * 's/\./YYY/g' | sed 's/-/ZZZ/g' | sed 's/%/XXX/g'
	 * 
	 * @param name
	 * @return
	 */
	private static String encodeSpecialMdxCharactersInNames(String name) {
		try {
			name = URLEncoder.encode(name, "UTF-8");
			name = name.replace("%", "XXX");
			name = name.replace(".", "YYY");
			name = name.replace("-", "ZZZ");
			return name;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	static String decodeSpecialMdxCharactersInNames(String name) {
		try {
			name = name.replace("XXX", "%");
			name = name.replace("YYY", ".");
			name = name.replace("ZZZ", "-");
			name = URLDecoder.decode(name, "UTF-8");
			return name;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * This helper class makes the use of nx parser easier. It creates a map
	 * between the strings used in the given header and the column number where
	 * a name appears.
	 * 
	 * @param header
	 * @return map
	 */
	public static Map<String, Integer> getNodeResultFields(
			org.semanticweb.yars.nx.Node[] header) {
		Map<String, Integer> mapFields = new HashMap<String, Integer>();
		for (int i = 0; i < header.length; i++) {
			// Is variable, therefore, simply toString.
			String nodeString = header[i].toString();
			// TODO: For now, I add the question mark, again, although, it would
			// not be needed
			mapFields.put("?" + nodeString, i);
		}
		return mapFields;
	}
	
	public static List<List<Node[]>> splitandparseN3rule(String n3rule) throws IOException {
		// Remove {, }
		
		n3rule = n3rule.replace("{", "");
		n3rule = n3rule.replace("}", "");
				
		// Take => as split.
		
		int n3ruleindex = n3rule.indexOf("=>");
		
		String n3rule_body = n3rule.substring(0, n3ruleindex);
		String n3rule_head = n3rule.substring(n3ruleindex+2, n3rule.length());
		
		InputStream stream = new ByteArrayInputStream(n3rule_body.getBytes("UTF-8"));
		NxParser nxp = new NxParser(stream);

		List<Node[]> n3rule_nodes_body = new ArrayList<Node[]>();

		Node[] nxx;
		while (nxp.hasNext()) {
			nxx = nxp.next();
			n3rule_nodes_body.add(nxx);
			for (Node node : nxx) {
				System.out.print(node.toN3()+ " ");
			}
			System.out.println();
		}
		stream.close();
			
		stream = new ByteArrayInputStream(n3rule_head.getBytes("UTF-8"));
		nxp = new NxParser(stream);

		List<Node[]> n3rule_nodes_head = new ArrayList<Node[]>();

		while (nxp.hasNext()) {
			nxx = nxp.next();
			n3rule_nodes_head.add(nxx);
			for (Node node : nxx) {
				System.out.print(node.toN3()+" ");
			}
			System.out.println();
		}
		stream.close();
		
		List<List<Node[]>> result = new ArrayList<List<Node[]>>();
		result.add(n3rule_nodes_body);
		result.add(n3rule_nodes_head);
		
		return result;
	}

	public static InputStream transformSparqlXmlToNx(InputStream xml) {

		javax.xml.transform.TransformerFactory tf = javax.xml.transform.TransformerFactory
				.newInstance("net.sf.saxon.TransformerFactoryImpl", Thread
						.currentThread().getContextClassLoader());

		Transformer t;

		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		try {
			// String base = System.getProperty("RESOURCE_BASE", "");
			// String userDir = System.getProperty("user.home");
			// TODO: This is not very generic.

			// Instead of:
			// File xslFile = new File(
			// "C:/Users/b-kaempgen/Documents/Workspaces/Eclipse_SLD/OLAP4LD/resources/xml2nx.xsl");
			// t = tf.newTransformer(new StreamSource(xslFile.getPath()));

			t = tf.newTransformer(new StreamSource(Olap4ldLinkedDataUtil.class
					.getResourceAsStream("/xml2nx.xsl")));

			StreamSource ssource = new StreamSource(xml);
			StreamResult sresult = new StreamResult(baos);

			Olap4ldUtil._log.config("...applying xslt to transform xml to nx...");

			t.transform(ssource, sresult);

			// We need to make INputStream out of OutputStream
			ByteArrayInputStream nx = new ByteArrayInputStream(
					baos.toByteArray());

			return nx;
		} catch (TransformerException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
	/**
	 * dump RDF graph
	 * 
	 * @param out
	 *            output stream for the serialization
	 * @param outform
	 *            the RDF serialization format for the dump
	 * @return
	 */
	public static void dumpRDF(SailRepository repo, String file, RDFFormat outform) {
		try {
			// dump the graph in the specified format
			System.out.println("\n==GRAPH DUMP==\n");
			FileOutputStream fos = new FileOutputStream(file);
			RepositoryConnection con = repo.getConnection();
			try {
				RDFWriter w = Rio.createWriter(outform, fos);
				con.export(w);
			} finally {
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param repo
	 * @param query
	 * @param caching
	 */
	public static void sparqlRepoUpdate(SailRepository repo, String query, boolean caching) {
		Olap4ldUtil._log.config("SPARQL update query: " + query);

		try {
			RepositoryConnection con = repo.getConnection();

			Update tupleQuery = con.prepareUpdate(QueryLanguage.SPARQL, query);
			tupleQuery.execute();

			// do something interesting with the values here...
			// con.close();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UpdateExecutionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
}
