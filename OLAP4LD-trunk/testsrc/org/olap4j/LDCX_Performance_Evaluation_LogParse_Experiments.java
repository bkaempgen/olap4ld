package org.olap4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.kit.aifb.exrunner.ExperimentRun;
import edu.kit.aifb.exrunner.model.ExperimentSystem;
import edu.kit.aifb.exrunner.model.ParameterSet;
import edu.kit.aifb.exrunner.model.ParameterSetProvider;
import edu.kit.aifb.exrunner.model.attribute.Attribute;
import edu.kit.aifb.exrunner.model.attribute.IntAttribute;
import edu.kit.aifb.exrunner.model.attribute.StringAttribute;

/**
 * Parse an Apache log file with StringTokenizer
 */
public class LDCX_Performance_Evaluation_LogParse_Experiments {

	/*
	 * We are interested in creating such a table: querytime | queryname |
	 * triples | loadingvalidatingdataset | generatinglogicalqueryplan |
	 * executinglogicalqueryplan
	 */
	public static class LDCX_Query_Attributes {
		public static final Attribute QUERYTIME = new StringAttribute(
				"querytime");
		public static final Attribute QUERYNAME = new StringAttribute(
				"queryname");
		public static final Attribute TRIPLES = new IntAttribute("triples");
		public static final Attribute LOADINGVALIDATINGDATASET = new IntAttribute(
				"loadingvalidatingdataset");
		public static final Attribute GENERATINGLOGICALQUERYPLAN = new IntAttribute(
				"generatinglogicalqueryplan");
		public static final Attribute EXECUTINGLOGICALQUERYPLAN = new IntAttribute(
				"executinglogicalqueryplan");
		public static final Attribute TRANSMISSIONTIME = new IntAttribute(
				"userquerytime");
	}

	public static class LDCX_ParameterSetProvider extends ParameterSetProvider {

		public LDCX_ParameterSetProvider() {
			registerAttributes(LDCX_Query_Attributes.QUERYTIME,
					LDCX_Query_Attributes.QUERYNAME,
					LDCX_Query_Attributes.TRIPLES,
					LDCX_Query_Attributes.LOADINGVALIDATINGDATASET,
					LDCX_Query_Attributes.GENERATINGLOGICALQUERYPLAN,
					LDCX_Query_Attributes.EXECUTINGLOGICALQUERYPLAN,
					LDCX_Query_Attributes.TRANSMISSIONTIME);
		}

		@Override
		public List<ParameterSet> getParameterSets() {

			List<ParameterSet> params = Lists.newArrayList();

			// Parse log file and add.
			List<QueryLog> queryloglist = parseLogs();

			// Now we have all querylogs
			System.out
					.println("Querytime | Query | Triples | Loading and validating dataset | Generating logical query plan | Executing logical query plan | Transmission");
			for (QueryLog queryLog : queryloglist) {
				params.add(new QueryLogParameterSet(queryLog));
			}
			return params;
		}

		/**
		 * Parses olap4ld logs via regex specified for info logs.
		 * @return
		 */
		public List<QueryLog> parseLogs() {

			List<QueryLog> queryloglist = new ArrayList<QueryLog>();

			// Example
			// Pattern p = Pattern.compile("^[a-zA-Z]+([0-9]+).*");
			// Matcher m = p.matcher("Testing123Testing");
			//
			// if (m.find()) {
			// System.out.println(m.group(1));
			// }

			Scanner s;
			try {
				s = new Scanner(SCANNEDFILE);

				QueryLog query = new QueryLog();

				System.out.println("Lines");
				// Full example

				while (s.hasNextLine()) {
					String line = s.nextLine();
					// System.out.println(line);

					// Oct 19, 2013 9:19:24 PM
					// org.olap4j.driver.olap4ld.Olap4ldStatement
					// executeOlapQuery
					if (line.contains("org.olap4j.driver.olap4ld.Olap4ldStatement executeOlapQuery")) {

						query.querytime = line
								.replace(
										"org.olap4j.driver.olap4ld.Olap4ldStatement executeOlapQuery",
										"");
						continue;
					}

					// INFO: Parse MDX: SELECT /* $session:
					// ldcx_performance_evaluation_testSsbOlapCustomers */ NON
					// EMPTY
					// {[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_discount],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_extendedprice],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_quantity],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_revenue],[httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_supplycost]}
					// ON COLUMNS, NON EMPTY
					// {Members([httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23lo_custkeyCodeList])}
					// ON ROWS FROM
					// [httpXXX3AXXX2FXXX2Folap4ldYYYgooglecodeYYYcomXXX2FgitXXX2FOLAP4LDZZZtrunkXXX2FtestsXXX2Fssb001XXX2FttlXXX2FexampleYYYttlXXX23ds]
					if (line.contains("Parse MDX: SELECT ")) {

						Pattern p = Pattern.compile("^.*/\\*(.+)\\*/.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							System.out.println(m.group(1));
							query.queryname = new String(m.group(1));
							// We only use distinguishing part
							query.queryname = query.queryname.replace(
									"$session: ldcx_performance_evaluation_",
									"");
						}
						continue;
					}

					// INFO: Parse MDX: finished in 97ms.
					if (line.contains("Parse MDX: finished")) {

						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							System.out.println(m.group(1));
							query.parsemdx = new Integer(m.group(1));
						}
						continue;
					}

					// INFO: Execute metadata query on Linked Data Engine:
					// finished
					// in 4038ms.
					if (line.contains("Execute metadata query on Linked Data Engine: finished")) {

						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							System.out.println(m.group(1));
							if (query.executemetadataqueries == -1) {
								query.executemetadataqueries = new Integer(
										m.group(1));
							} else {
								query.executemetadataqueries += new Integer(
										m.group(1));
							}

						}
						continue;
					}

					// INFO: Load dataset: loading 175 triples finished in
					// 3933ms.
					if (line.contains("Load dataset:")) {

						Pattern p = Pattern
								.compile("^.*loading (.+) triples finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {

							System.out.println(m.group(1));
							query.triples = new Integer(m.group(1));

							System.out.println(m.group(2));
							query.loaddataset = new Integer(m.group(2));
						}
					}

					// INFO: Run normalisation algorithm on dataset: finished in
					// 17ms.
					if (line.contains("Run normalisation algorithm on dataset: finished")) {

						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							System.out.println(m.group(1));
							query.normalisation = new Integer(m.group(1));
						}
						continue;
					}

					// INFO: Check integrity constraints on dataset: finished in
					// 70ms.
					if (line.contains("Check integrity constraints on dataset: finished")) {

						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							System.out.println(m.group(1));
							query.integrityconstraints = new Integer(m.group(1));
						}
						continue;
					}

					// INFO: Transform MDX parse tree: finished in 5131ms.
					if (line.contains("Transform MDX parse tree: finished")) {

						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							System.out.println(m.group(1));
							query.transformmdxparsetree = new Integer(
									m.group(1));
						}
					}

					// INFO: Create and execute physical query plan: finished in
					// 65ms.
					// TODO: We do not need to separate.
					// INFO: Execute logical query plan: finished in 74ms.
					if (line.contains("Execute logical query plan: finished")) {

						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							System.out.println(m.group(1));
							query.executelogicalqueryplan = new Integer(
									m.group(1));
						}
						
						// Now, we can create a new query log
						queryloglist.add(query);
						query = new QueryLog();

						continue;
					}
					
//					// This information is not available from olap4ld log directly. Instead, we compute the userquery time by waiting until the next query can be issued.
//					if (line.contains("Run performance evaluation query: finished")) {
//
//						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
//						Matcher m = p.matcher(line);
//
//						if (m.find()) {
//							System.out.println(m.group(1));
//							query.userquerytime = new Integer(
//									m.group(1));
//						}
//
//						
//
//						continue;
//					}

				}

				s.close();

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return queryloglist;
		}

	}

	public static class QueryLogParameterSet extends ParameterSet {

		public QueryLogParameterSet(QueryLog queryLog) {

			
			// Computes the necessary metrics
			
			m_attrValues.put(LDCX_Query_Attributes.QUERYTIME,
					queryLog.querytime);
			m_attrValues.put(LDCX_Query_Attributes.QUERYNAME,
					queryLog.queryname);
			m_attrValues.put(LDCX_Query_Attributes.TRIPLES, queryLog.triples);
			int loadingandvalidatingdataset = queryLog.loaddataset
					+ queryLog.normalisation + queryLog.integrityconstraints;

			m_attrValues.put(LDCX_Query_Attributes.LOADINGVALIDATINGDATASET,
					loadingandvalidatingdataset);
			int generatinglogicalqueryplan = queryLog.parsemdx
					+ queryLog.transformmdxparsetree
					- loadingandvalidatingdataset;
			m_attrValues.put(LDCX_Query_Attributes.GENERATINGLOGICALQUERYPLAN,
					generatinglogicalqueryplan);

			int executinglogicalqueryplan = queryLog.executelogicalqueryplan;
			m_attrValues.put(LDCX_Query_Attributes.EXECUTINGLOGICALQUERYPLAN,
					executinglogicalqueryplan);
			
			int transmissiontime = queryLog.userquerytime - loadingandvalidatingdataset - generatinglogicalqueryplan - executinglogicalqueryplan;
			m_attrValues.put(LDCX_Query_Attributes.TRANSMISSIONTIME,
					transmissiontime);
		}
	}

	public static class Bottleneck_ExperimentSystem extends ExperimentSystem {

		public Bottleneck_ExperimentSystem() {
			super("bottleneck");
		}

		@Override
		public Map<Attribute, Object> open() throws IOException {
			return ImmutableMap.<Attribute, Object> of();
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public Map<Attribute, Object> execute(ParameterSet query)
				throws IOException {

			return ImmutableMap.<Attribute, Object> of();
		}

	}
	
	/**
	 * Common fields for Apache Log demo.
	 */
	public static class QueryLog {
		// Ordered as happening in stream
		String querytime = "none";
		String queryname = "none";
		int parsemdx = -1;
		int executemetadataqueries = -1;
		int loaddataset = -1;
		int normalisation = -1;
		int integrityconstraints = -1;
		int triples = -1;
		int transformmdxparsetree = -1;
		int createandexecutephysicalqueryplan = -1;
		int executelogicalqueryplan = -1;
		int userquerytime = -1;
	}

	private static final File SCANNEDFILE = new File("/home/benedikt/Workspaces/Git-Repositories/olap4ld/OLAP4LD-trunk/testresources/olap4ld_0.log");
	//private static final File SCANNEDFILE = new File("/media/84F01919F0191352/Projects/2013/paper/paper-olap4ld-demo/Experiments/PerformanceStudy/catalina_bigger_5_runs_test_including_slice_2013-10-22_v2.out");
	//private static final File SCANNEDFILE = new File("/media/84F01919F0191352/Projects/2013/paper/paper-olap4ld-demo/Experiments/PerformanceStudy/catalina_bigger_5_runs_test_2013-10-20.out");
			
	
	private static final File SQLITEFILE = new File("/home/benedikt/Workspaces/Git-Repositories/olap4ld/OLAP4LD-trunk/testresources/bottleneck.db");

	public static void main(String argv[]) throws ClassNotFoundException,
			SQLException, IOException {

		ExperimentRun run1 = new ExperimentRun(
				new Bottleneck_ExperimentSystem(),
				new LDCX_ParameterSetProvider(), 1, SQLITEFILE,
				null);
		run1.run();

	}
}

