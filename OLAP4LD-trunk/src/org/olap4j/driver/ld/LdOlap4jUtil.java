/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */
package org.olap4j.driver.ld;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.xerces.impl.Constants;
import org.apache.xerces.parsers.DOMParser;
import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Resource;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.SAXParseException;

/**
 * Utility methods for the olap4j driver for XML/A.
 * 
 * <p>
 * Many of the methods are related to XML parsing. For general-purpose methods
 * useful for implementing any olap4j driver, see the org.olap4j.impl package
 * and in particular {@link org.olap4j.impl.Olap4jUtil}.
 * 
 * @author jhyde
 * @version $Id: XmlaOlap4jUtil.java 315 2010-05-29 00:56:11Z jhyde $
 * @since Dec 2, 2007
 */
public abstract class LdOlap4jUtil {
	// Logging?
	static Logger _log = Logger.getLogger("XmlaOlap4jUtil");

	/*
	 * XMLA prefixes
	 */
	static final String LINE_SEP = System.getProperty("line.separator", "\n");
	static final String SOAP_PREFIX = "SOAP-ENV";
	static final String SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/";
	static final String XMLA_PREFIX = "xmla";
	static final String XMLA_NS = "urn:schemas-microsoft-com:xml-analysis";
	static final String MDDATASET_NS = "urn:schemas-microsoft-com:xml-analysis:mddataset";
	static final String ROWSET_NS = "urn:schemas-microsoft-com:xml-analysis:rowset";

	static final String XSD_PREFIX = "xsd";
	static final String XMLNS = "xmlns";

	/*
	 * RDF prefixes
	 */
	static HashMap<Integer, String> standard_prefix2uri = null;
	static HashMap<Integer, String> standard_uri2prefix = null;

	static final String NAMESPACES_FEATURE_ID = "http://xml.org/sax/features/namespaces";
	static final String VALIDATION_FEATURE_ID = "http://xml.org/sax/features/validation";
	static final String SCHEMA_VALIDATION_FEATURE_ID = "http://apache.org/xml/features/validation/schema";
	static final String FULL_SCHEMA_VALIDATION_FEATURE_ID = "http://apache.org/xml/features/validation/schema-full-checking";
	static final String DEFER_NODE_EXPANSION = "http://apache.org/xml/features/dom/defer-node-expansion";
	static final String SCHEMA_LOCATION = Constants.XERCES_PROPERTY_PREFIX
			+ Constants.SCHEMA_LOCATION;

	static InputStream transformSparqlXmlToNx(InputStream xml) {

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

			t = tf.newTransformer(new StreamSource(LdOlap4jUtil.class
					.getResourceAsStream("/xml2nx.xsl")));

			StreamSource ssource = new StreamSource(xml);
			StreamResult sresult = new StreamResult(baos);

			_log.info("...applying xslt to transform xml to nx...");
			// System.out.println("herwe");

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
	 * Parse a stream into a Document (no validation).
	 * 
	 */
	static Document parse(byte[] in) throws SAXException, IOException {
		InputSource source = new InputSource(new ByteArrayInputStream(in));

		DOMParser parser = getParser(null, null, false);
		try {
			parser.parse(source);
			checkForParseError(parser);
		} catch (SAXParseException ex) {
			checkForParseError(parser, ex);
		}

		return parser.getDocument();
	}

	/**
	 * Get your non-cached DOM parser which can be configured to do schema based
	 * validation of the instance Document.
	 * 
	 */
	static DOMParser getParser(String schemaLocationPropertyValue,
			EntityResolver entityResolver, boolean validate)
			throws SAXNotRecognizedException, SAXNotSupportedException {
		boolean doingValidation = (validate || (schemaLocationPropertyValue != null));

		DOMParser parser = new DOMParser();

		parser.setEntityResolver(entityResolver);
		parser.setErrorHandler(new ErrorHandlerImpl());
		parser.setFeature(DEFER_NODE_EXPANSION, false);
		parser.setFeature(NAMESPACES_FEATURE_ID, true);
		parser.setFeature(SCHEMA_VALIDATION_FEATURE_ID, doingValidation);
		parser.setFeature(VALIDATION_FEATURE_ID, doingValidation);

		if (schemaLocationPropertyValue != null) {
			parser.setProperty(SCHEMA_LOCATION,
					schemaLocationPropertyValue.replace('\\', '/'));
		}

		return parser;
	}

	/**
	 * Checks whether the DOMParser after parsing a Document has any errors and,
	 * if so, throws a RuntimeException exception containing the errors.
	 */
	static void checkForParseError(DOMParser parser, Throwable t) {
		final ErrorHandler errorHandler = parser.getErrorHandler();

		if (errorHandler instanceof ErrorHandlerImpl) {
			final ErrorHandlerImpl saxEH = (ErrorHandlerImpl) errorHandler;
			final List<ErrorInfo> errors = saxEH.getErrors();

			if (errors != null && errors.size() > 0) {
				String errorStr = ErrorHandlerImpl.formatErrorInfos(saxEH);
				throw new RuntimeException(errorStr, t);
			}
		} else {
			System.out.println("errorHandler=" + errorHandler);
		}
	}

	static void checkForParseError(final DOMParser parser) {
		checkForParseError(parser, null);
	}

	static List<Node> listOf(final NodeList nodeList) {
		return new AbstractList<Node>() {
			public Node get(int index) {
				return nodeList.item(index);
			}

			public int size() {
				return nodeList.getLength();
			}
		};
	}

	static String gatherText(Element element) {
		StringBuilder buf = new StringBuilder();
		final NodeList childNodes = element.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			buf.append(childNodes.item(i).getTextContent());
		}
		return buf.toString();
	}

	static String prettyPrint(Element element) {
		StringBuilder string = new StringBuilder();
		prettyPrintLoop(element, string, "");
		return string.toString();
	}

	private static void prettyPrintLoop(NodeList nodes, StringBuilder string,
			String indentation) {
		for (int index = 0; index < nodes.getLength(); index++) {
			prettyPrintLoop(nodes.item(index), string, indentation);
		}
	}

	private static void prettyPrintLoop(Node node, StringBuilder string,
			String indentation) {
		if (node == null) {
			return;
		}

		int type = node.getNodeType();
		switch (type) {
		case Node.DOCUMENT_NODE:
			string.append("\n");
			prettyPrintLoop(node.getChildNodes(), string, indentation + "\t");
			break;

		case Node.ELEMENT_NODE:
			string.append(indentation);
			string.append("<");
			string.append(node.getNodeName());

			Attr[] attributes;
			if (node.getAttributes() != null) {
				int length = node.getAttributes().getLength();
				attributes = new Attr[length];
				for (int loopIndex = 0; loopIndex < length; loopIndex++) {
					attributes[loopIndex] = (Attr) node.getAttributes().item(
							loopIndex);
				}
			} else {
				attributes = new Attr[0];
			}

			for (Attr attribute : attributes) {
				string.append(" ");
				string.append(attribute.getNodeName());
				string.append("=\"");
				string.append(attribute.getNodeValue());
				string.append("\"");
			}

			string.append(">\n");

			prettyPrintLoop(node.getChildNodes(), string, indentation + "\t");

			string.append(indentation);
			string.append("</");
			string.append(node.getNodeName());
			string.append(">\n");

			break;

		case Node.TEXT_NODE:
			string.append(indentation);
			string.append(node.getNodeValue().trim());
			string.append("\n");
			break;

		case Node.PROCESSING_INSTRUCTION_NODE:
			string.append(indentation);
			string.append("<?");
			string.append(node.getNodeName());
			String text = node.getNodeValue();
			if (text != null && text.length() > 0) {
				string.append(text);
			}
			string.append("?>\n");
			break;

		case Node.CDATA_SECTION_NODE:
			string.append(indentation);
			string.append("<![CDATA[");
			string.append(node.getNodeValue());
			string.append("]]>");
			break;
		}
	}

	static Element findChild(Element element, String ns, String tag) {
		final NodeList childNodes = element.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			if (childNodes.item(i) instanceof Element) {
				Element child = (Element) childNodes.item(i);
				if (child.getLocalName().equals(tag)
						&& (ns == null || child.getNamespaceURI().equals(ns))) {
					return child;
				}
			}
		}
		return null;
	}

	static String stringElement(Element row, String name) {
		final NodeList childNodes = row.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			final Node node = childNodes.item(i);
			if (name.equals(node.getLocalName())) {
				return node.getTextContent();
			}
		}
		return null;
	}

	static Integer integerElement(Element row, String name) {
		final String s = stringElement(row, name);
		if (s == null || s.equals("")) {
			return null;
		} else {
			return Integer.valueOf(s);
		}
	}

	static int intElement(Element row, String name) {
		return integerElement(row, name).intValue();
	}

	static Double doubleElement(Element row, String name) {
		return Double.valueOf(stringElement(row, name));
	}

	static boolean booleanElement(Element row, String name) {
		return "true".equals(stringElement(row, name));
	}

	static Float floatElement(Element row, String name) {
		return Float.valueOf(stringElement(row, name));
	}

	static long longElement(Element row, String name) {
		return Long.valueOf(stringElement(row, name)).longValue();
	}

	static List<Element> childElements(Element memberNode) {
		final List<Element> list = new ArrayList<Element>();
		final NodeList childNodes = memberNode.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); ++i) {
			final Node childNode = childNodes.item(i);
			if (childNode instanceof Element) {
				list.add((Element) childNode);
			}
		}
		return list;
	}

	static List<Element> findChildren(Element element, String ns, String tag) {
		final List<Element> list = new ArrayList<Element>();
		for (Node node : listOf(element.getChildNodes())) {
			if (tag.equals(node.getLocalName())
					&& ((ns == null) || node.getNamespaceURI().equals(ns))) {
				list.add((Element) node);
			}
		}
		return list;
	}

	/**
	 * Converts a Node to a String.
	 * 
	 * @param node
	 *            XML node
	 * @param prettyPrint
	 *            Whether to print with nice indentation
	 * @return String representation of XML
	 */
	public static String toString(Node node, boolean prettyPrint) {
		if (node == null) {
			return null;
		}
		try {
			Document doc = node.getOwnerDocument();
			OutputFormat format;
			if (doc != null) {
				format = new OutputFormat(doc, null, prettyPrint);
			} else {
				format = new OutputFormat("xml", null, prettyPrint);
			}
			if (prettyPrint) {
				format.setLineSeparator(LINE_SEP);
			} else {
				format.setLineSeparator("");
			}
			StringWriter writer = new StringWriter(1000);
			XMLSerializer serial = new XMLSerializer(writer, format);
			serial.asDOMSerializer();
			if (node instanceof Document) {
				serial.serialize((Document) node);
			} else if (node instanceof Element) {
				format.setOmitXMLDeclaration(true);
				serial.serialize((Element) node);
			} else if (node instanceof DocumentFragment) {
				format.setOmitXMLDeclaration(true);
				serial.serialize((DocumentFragment) node);
			} else if (node instanceof Text) {
				Text text = (Text) node;
				return text.getData();
			} else if (node instanceof Attr) {
				Attr attr = (Attr) node;
				String name = attr.getName();
				String value = attr.getValue();
				writer.write(name);
				writer.write("=\"");
				writer.write(value);
				writer.write("\"");
				if (prettyPrint) {
					writer.write(LINE_SEP);
				}
			} else {
				writer.write("node class = " + node.getClass().getName());
				if (prettyPrint) {
					writer.write(LINE_SEP);
				} else {
					writer.write(' ');
				}
				writer.write("XmlUtil.toString: fix me: ");
				writer.write(node.toString());
				if (prettyPrint) {
					writer.write(LINE_SEP);
				}
			}
			return writer.toString();
		} catch (Exception ex) {
			// ignore
			return null;
		}
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

	/**
	 * Error handler plus helper methods.
	 */
	static class ErrorHandlerImpl implements ErrorHandler {
		public static final String WARNING_STRING = "WARNING";
		public static final String ERROR_STRING = "ERROR";
		public static final String FATAL_ERROR_STRING = "FATAL";

		// DOMError values
		public static final short SEVERITY_WARNING = 1;
		public static final short SEVERITY_ERROR = 2;
		public static final short SEVERITY_FATAL_ERROR = 3;

		public void printErrorInfos(PrintStream out) {
			if (errors != null) {
				for (ErrorInfo error : errors) {
					out.println(formatErrorInfo(error));
				}
			}
		}

		public static String formatErrorInfos(ErrorHandlerImpl saxEH) {
			if (!saxEH.hasErrors()) {
				return "";
			}
			StringBuilder buf = new StringBuilder(512);
			for (ErrorInfo error : saxEH.getErrors()) {
				buf.append(formatErrorInfo(error));
				buf.append(LINE_SEP);
			}
			return buf.toString();
		}

		public static String formatErrorInfo(ErrorInfo ei) {
			StringBuilder buf = new StringBuilder(128);
			buf.append("[");
			switch (ei.severity) {
			case SEVERITY_WARNING:
				buf.append(WARNING_STRING);
				break;
			case SEVERITY_ERROR:
				buf.append(ERROR_STRING);
				break;
			case SEVERITY_FATAL_ERROR:
				buf.append(FATAL_ERROR_STRING);
				break;
			}
			buf.append(']');
			String systemId = ei.exception.getSystemId();
			if (systemId != null) {
				int index = systemId.lastIndexOf('/');
				if (index != -1) {
					systemId = systemId.substring(index + 1);
				}
				buf.append(systemId);
			}
			buf.append(':');
			buf.append(ei.exception.getLineNumber());
			buf.append(':');
			buf.append(ei.exception.getColumnNumber());
			buf.append(": ");
			buf.append(ei.exception.getMessage());
			return buf.toString();
		}

		private List<ErrorInfo> errors;

		public ErrorHandlerImpl() {
		}

		public List<ErrorInfo> getErrors() {
			return this.errors;
		}

		public boolean hasErrors() {
			return (this.errors != null);
		}

		public void warning(SAXParseException exception) throws SAXException {
			addError(new ErrorInfo(SEVERITY_WARNING, exception));
		}

		public void error(SAXParseException exception) throws SAXException {
			addError(new ErrorInfo(SEVERITY_ERROR, exception));
		}

		public void fatalError(SAXParseException exception) throws SAXException {
			addError(new ErrorInfo(SEVERITY_FATAL_ERROR, exception));
		}

		protected void addError(ErrorInfo ei) {
			if (this.errors == null) {
				this.errors = new ArrayList<ErrorInfo>();
			}
			this.errors.add(ei);
		}

		public String getFirstError() {
			return (hasErrors()) ? formatErrorInfo(errors.get(0)) : "";
		}
	}

	static class ErrorInfo {
		final SAXParseException exception;
		final short severity;

		ErrorInfo(short severity, SAXParseException exception) {
			this.severity = severity;
			this.exception = exception;
		}
	}

	/**
	 * Elements (identifiers) used in MDX need to follow certain rules in order
	 * to be parseable from and to MDX. In fact, having it URL encoded is not
	 * enough, also, % and . need to be replaced.
	 * 
	 * @param baseuri
	 *            The pure uri string to be encoded for MDX
	 * @return encoded string
	 */
	public static String encodeUriWithPrefix(String uri) {

		if (standard_prefix2uri == null && standard_uri2prefix == null) {
			readInStandardPrefixes();
		}

		/*
		 * First, I have to retrieve the base uri. If there is a hash, I just
		 * remove it. If there is no hash, I remove everything from the last /
		 * on.
		 */
		String qname = "";
		String baseuri = "";
		int lastIndexHash = uri.lastIndexOf("#");
		if (lastIndexHash != -1) {
			qname = uri.substring(lastIndexHash + 1);
			baseuri = uri.substring(0, lastIndexHash + 1);
		} else {
			int lastIndexSlash = uri.lastIndexOf("/");
			qname = uri.substring(lastIndexSlash + 1);
			baseuri = uri.substring(0, lastIndexSlash + 1);
		}

		// If there is no prefix stored, we have to come up with one ourselves
		// (which if possible should be always the same):
		String prefix = standard_uri2prefix.get(baseuri.hashCode());
		if (prefix == null) {
			// BASE64Encoder myEncoder = new BASE64Encoder();
			// String encodedURI = myEncoder.encode(baseuri.getBytes());
			// encodedURI = encodedURI.replace("/(\r\n|\n|\r)/gm","");
			// Base64 myEncoder = new Base64(0);
			// String encodedURI = myEncoder.encodeToString(baseuri.getBytes());
			try {
				prefix = URLEncoder.encode(baseuri, "UTF-8");
				/*
				 * I also encode the following:
				 * 
				 * % = XXX . = YYY
				 */
				prefix = prefix.replace("%", "XXX");
				prefix = prefix.replace(".", "YYY");
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return prefix + ":" + qname;
		} else {
			return prefix + ":" + qname;
		}
	}

	/**
	 * Decode an MDX identifier of a URI.
	 * 
	 * Before using this method, we need to make sure that MDX has not wrapped
	 * the URI with square brackets, e.g., [Measures].[http...].
	 * 
	 * @param uri
	 * @return
	 */
	public static String decodeUriWithPrefix(String prefixeduri) {

		if (standard_prefix2uri == null && standard_uri2prefix == null) {
			readInStandardPrefixes();
		}

		// In this case, the : is the sign
		int lastIndexColon = prefixeduri.lastIndexOf(":");
		String qname = prefixeduri.substring(lastIndexColon + 1);
		// Without colon
		String prefix = prefixeduri.substring(0, lastIndexColon);

		String prefixuri = standard_prefix2uri.get(prefix.hashCode());

		// Possibly stored in specific prefixes.
		if (prefixuri == null) {
			// BASE64Decoder myDecoder = new BASE64Decoder();
			// prefixuri = new String(myDecoder.decodeBuffer(prefix));
			// Base64 myDecoder = new Base64(0);
			// prefixuri = new String(myDecoder.decode(prefix.getBytes()));
			try {
				/*
				 * I first decode the following:
				 * 
				 * % = XXX . = YYY
				 */
				prefix = prefix.replace("XXX", "%");
				prefix = prefix.replace("YYY", ".");
				prefixuri = new String(URLDecoder.decode(prefix, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return prefixuri + qname;
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
		final String myValue;
		// If value is uri, then convert into MDX friendly format
		if (node.toString().equals("null")) {
			myValue = null;
		} else if (node instanceof Literal) {
			// For literals, we do not need prefixes, therefore simple string
			// notation.
			String value = node.toString();
			/*
			 * TODO: Problem: SPARQL does not return URI when it should,
			 * therefore we take everything as a resource what starts with
			 * http://
			 */
			if (value.contains("http://")) {
				value = LdOlap4jUtil.encodeUriWithPrefix(node.toString());
			}
			myValue = value;

		} else if (node instanceof Resource) {
			// For uris, we want to have prefixed notation.
			myValue = LdOlap4jUtil.encodeUriWithPrefix(node.toString());
		} else {
			LdOlap4jUtil._log
					.severe("Here, we do not know what to do with a result node from Linked Data Engine.");
			myValue = null;
		}

		// We wrap any node value with brackets.
		return "[" + myValue + "]";
	}

	public static String convertMDXtoURI(String mdx) {
		// First, we remove the squared brackets
		mdx = mdx.substring(1, mdx.length() - 1);

		// check whether prefix notation
		if (mdx.contains("httpXXX3AXXX2FXXX2F")) {
			return decodeUriWithPrefix(mdx);
		} else if (mdx.lastIndexOf(":") != -1) {
			return decodeUriWithPrefix(mdx);
		} else {
			// no conversion needed
			return mdx;
		}
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
					LdOlap4jUtil.class
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
	 * TODO: At the moment, I do not have a good way of dealing with captions.
	 * Therefore this method that converts a should-be-caption into a proper
	 * caption.
	 * 
	 * @param caption
	 * @return
	 */
	public static String makeCaption(String caption) {
		// A caption should never be null
		if (caption == null) {
			return "";
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
	 * @param name
	 * @return
	 */
	public static String removeSquareBrackets(String name) {
		if (!name.startsWith("[") || !name.endsWith("]")) {
			throw new UnsupportedOperationException(
					"Name not surrounded by square brackets!");
		} else {
			return name.substring(1, name.length()-1);
		}
	}

}

// End XmlaOlap4jUtil.java
