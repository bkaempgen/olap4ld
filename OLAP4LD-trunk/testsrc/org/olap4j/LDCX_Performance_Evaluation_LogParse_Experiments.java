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
	 * We are interested in creating such a table:
	 */
	public static class LDCX_Query_Attributes {
		public static final Attribute QUERYTIME = new StringAttribute(
				"querytime");
		public static final Attribute QUERYNAME = new StringAttribute(
				"queryname");
		public static final Attribute DATASETSCOUNT = new IntAttribute(
				"datasetscount");

		public static final Attribute TRIPLESCOUNT = new IntAttribute(
				"triplescount");
		public static final Attribute OBSERVATIONSCOUNT = new IntAttribute(
				"observationscount");

		public static final Attribute LOOKUPSCOUNT = new IntAttribute(
				"lookupscount");

		public static final Attribute LOADVALIDATEDATASETSTIME = new IntAttribute(
				"loadvalidatedatasetstime");

		public static final Attribute EXECUTEMETADATAQUERIESTIME = new IntAttribute(
				"executemetadataqueriestime");

		public static final Attribute EXECUTEMETADATAQUERIESCOUNT = new IntAttribute(
				"executemetadataqueriescount");

		public static final Attribute GENERATELOGICALQUERYPLANTIME = new IntAttribute(
				"generatelogicalqueryplantime");

		public static final Attribute GENERATEPHYSICALQUERYPLANTIME = new IntAttribute(
				"generatephysicalqueryplantime");

		public static final Attribute EXECUTEPHYSICALQUERYPLAN = new IntAttribute(
				"executephysicalqueryplantime");
	}

	public static class LDCX_ParameterSetProvider extends ParameterSetProvider {

		public LDCX_ParameterSetProvider() {
			registerAttributes(LDCX_Query_Attributes.QUERYTIME,
					LDCX_Query_Attributes.QUERYNAME,
					LDCX_Query_Attributes.DATASETSCOUNT,
					LDCX_Query_Attributes.TRIPLESCOUNT,
					LDCX_Query_Attributes.OBSERVATIONSCOUNT,
					LDCX_Query_Attributes.LOOKUPSCOUNT,
					LDCX_Query_Attributes.LOADVALIDATEDATASETSTIME,
					LDCX_Query_Attributes.EXECUTEMETADATAQUERIESTIME,
					LDCX_Query_Attributes.EXECUTEMETADATAQUERIESCOUNT,
					LDCX_Query_Attributes.GENERATELOGICALQUERYPLANTIME,
					LDCX_Query_Attributes.GENERATEPHYSICALQUERYPLANTIME,
					LDCX_Query_Attributes.EXECUTEPHYSICALQUERYPLAN);
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
		 * 
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

				boolean complete = false;
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
						complete = false;
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

						// Complete would be:
						// "^INFO: Parse MDX: SELECT /\\\\*(.+)\\\\*/.*"
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
							query.parsemdxtime = new Integer(m.group(1));
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
							if (query.executemetadataqueriestime == -1) {
								query.executemetadataqueriestime = new Integer(
										m.group(1));
								query.executemetadataqueriescount = 1;
							} else {
								// Increase
								query.executemetadataqueriestime += new Integer(
										m.group(1));
								query.executemetadataqueriescount++;
							}

						}
						continue;
					}

					// INFO: Load dataset: 2 datasets to load.
					Pattern myp = Pattern
							.compile("^.*Load dataset: (.+) datasets crawled.*");
					Matcher mym = myp.matcher(line);
					if (mym.find()) {
						System.out.println(mym.group(1));
						if (query.datasetscount < (new Integer(mym.group(1)))) {
							query.datasetscount = new Integer(mym.group(1));
						}
						continue;
					}

					// INFO: Load dataset: loading 175 triples finished in
					// 3933ms.
					if (line.contains("Load dataset: directed crawling algorithm finished in")) {

						// Two groups not used, anymore.
						// Pattern p = Pattern
						// .compile("^.*loading (.+) triples finished in (.+)ms.*");
						Pattern p = Pattern
								.compile("^.*directed crawling algorithm finished in (.+)ms.*");

						Matcher m = p.matcher(line);

						if (m.find()) {

							if (query.directedcrawlingdatasetstime == -1) {
								System.out.println("Directed crawling:"
										+ m.group(1));
								query.directedcrawlingdatasetstime = new Integer(
										m.group(1));
							} else {
								System.out.println("Directed crawling:"
										+ m.group(1));
								query.directedcrawlingdatasetstime += new Integer(
										m.group(1));
							}

						}
					}

					// INFO: Run normalisation algorithm on dataset: finished in
					// 17ms.
					if (line.contains("Run normalisation algorithm on dataset: finished")) {

						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {

							if (query.normalisationtime == -1) {
								System.out.println(m.group(1));
								query.normalisationtime = new Integer(
										m.group(1));
							} else {
								System.out.println(m.group(1));
								query.normalisationtime += new Integer(
										m.group(1));
							}
						}
						continue;
					}

					// INFO: Check integrity constraints on dataset: finished in
					// 70ms.
					if (line.contains("Check integrity constraints on dataset: finished")) {

						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							if (query.integrityconstraintstime == -1) {
								System.out.println(m.group(1));
								query.integrityconstraintstime = new Integer(
										m.group(1));
							} else {
								System.out.println(m.group(1));
								query.integrityconstraintstime += new Integer(
										m.group(1));
							}
						}
						continue;
					}

					// INFO: Lookup on resource: http://
					if (line.contains("Lookup on resource:")) {

						Pattern p = Pattern.compile("^.*Lookup on resource:.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							if (query.lookupscount == -1) {

								query.lookupscount = 1;
							} else {
								query.lookupscount++;
							}
						}
						continue;
					}

					// INFO: Load datasets: Number of loaded triples for all
					// datasets: 500
					if (line.contains("Load datasets: Number of loaded triples for all datasets:")) {

						Pattern p = Pattern
								.compile("^.*Load datasets: Number of loaded triples for all datasets: (.+)");
						Matcher m = p.matcher(line);

						if (m.find()) {

							// At the end will be largest one
							query.triplescount = new Integer(m.group(1));
						}
						continue;
					}

					// INFO: Load datasets: Number of observations for all
					// datasets: 500
					if (line.contains("Load datasets: Number of observations for all datasets:")) {

						Pattern p = Pattern
								.compile("^.*Load datasets: Number of observations for all datasets: (.+)");
						Matcher m = p.matcher(line);

						if (m.find()) {

							query.observationscount = new Integer(m.group(1));
						}
						continue;
					}

					// INFO: Transform MDX parse tree: finished in 5131ms.
					if (line.contains("Transform MDX parse tree: finished")) {

						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							System.out.println(m.group(1));
							query.transformmdxparsetreetime = new Integer(
									m.group(1));
						}
					}

					if (line.contains("Execute logical query plan: Generate physical query plan finished")) {

						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							System.out.println(m.group(1));
							query.generatephysicalqueryplantime = new Integer(
									m.group(1));
						}
					}

					if (line.contains("Execute logical query plan: Execute physical query plan finished in")) {

						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							System.out.println(m.group(1));
							query.executephysicalqueryplantime = new Integer(
									m.group(1));
						}

						continue;
					}

					if (line.contains("Execute logical query plan: Cache results finished in")) {

						Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
						Matcher m = p.matcher(line);

						if (m.find()) {
							System.out.println(m.group(1));
							query.cacheresultstime = new Integer(m.group(1));
						}

						complete = true;

						continue;
					}

					// // This information is not available from olap4ld log
					// directly. Instead, we compute the userquery time by
					// waiting until the next query can be issued.
					// if
					// (line.contains("Run performance evaluation query: finished"))
					// {
					//
					// Pattern p = Pattern.compile("^.*finished in (.+)ms.*");
					// Matcher m = p.matcher(line);
					//
					// if (m.find()) {
					// System.out.println(m.group(1));
					// query.userquerytime = new Integer(
					// m.group(1));
					// }
					//
					//
					//
					// continue;
					// }

					if (complete) {
						// Now, we can create a new query log
						queryloglist.add(query);
						query = new QueryLog();
					}

				}

				// Store all that has been collected, so far.
				if (!complete) {
					queryloglist.add(query);
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

			/*
			 * Computes the necessary metrics:
			 */

			m_attrValues.put(LDCX_Query_Attributes.QUERYTIME,
					queryLog.querytime);

			m_attrValues.put(LDCX_Query_Attributes.QUERYNAME,
					queryLog.queryname);

			m_attrValues.put(LDCX_Query_Attributes.TRIPLESCOUNT,
					queryLog.triplescount);

			m_attrValues.put(LDCX_Query_Attributes.DATASETSCOUNT,
					queryLog.datasetscount);

			m_attrValues.put(LDCX_Query_Attributes.OBSERVATIONSCOUNT,
					queryLog.observationscount);

			m_attrValues.put(LDCX_Query_Attributes.LOOKUPSCOUNT,
					queryLog.lookupscount);

			m_attrValues.put(LDCX_Query_Attributes.LOADVALIDATEDATASETSTIME,
					queryLog.directedcrawlingdatasetstime
							+ queryLog.normalisationtime
							+ queryLog.integrityconstraintstime);

			// Included getCubes, therefore remove dataset loading time from it
			m_attrValues
					.put(LDCX_Query_Attributes.EXECUTEMETADATAQUERIESTIME,
							queryLog.executemetadataqueriestime
									- (queryLog.directedcrawlingdatasetstime
											+ queryLog.normalisationtime + queryLog.integrityconstraintstime));

			m_attrValues.put(LDCX_Query_Attributes.EXECUTEMETADATAQUERIESCOUNT,
					queryLog.executemetadataqueriescount);

			m_attrValues.put(
					LDCX_Query_Attributes.GENERATELOGICALQUERYPLANTIME,
					queryLog.parsemdxtime + queryLog.transformmdxparsetreetime
							- queryLog.executemetadataqueriestime);

			m_attrValues.put(
					LDCX_Query_Attributes.GENERATEPHYSICALQUERYPLANTIME,
					queryLog.generatephysicalqueryplantime);

			m_attrValues.put(LDCX_Query_Attributes.EXECUTEPHYSICALQUERYPLAN,
					queryLog.executephysicalqueryplantime
							+ queryLog.cacheresultstime);

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

		// Name of query
		String queryname = "none";

		// Time parsing MDX
		int parsemdxtime = -1;

		// Metadata queries
		int executemetadataqueriestime = -1;
		int executemetadataqueriescount = -1;

		// Datasets
		int datasetscount = -1;

		// Crawling
		int directedcrawlingdatasetstime = -1;

		// Normalisation
		int normalisationtime = -1;

		// Validation
		int integrityconstraintstime = -1;

		// Lookups
		int lookupscount = -1;

		// Triples
		int triplescount = -1;

		// Observations
		int observationscount = -1;

		// Transform MDX parse tree to logical query plan
		int transformmdxparsetreetime = -1;

		// Execute logical query plan
		int generatephysicalqueryplantime = -1;

		int executephysicalqueryplantime = -1;

		int cacheresultstime = -1;
	}

	private static final File SCANNEDFILE = new File(
			"/home/benedikt/Workspaces/Git-Repositories/olap4ld/OLAP4LD-trunk/log/olap4ld_0.log");
	// private static final File SCANNEDFILE = new
	// File("/media/84F01919F0191352/Projects/2013/paper/paper-olap4ld-demo/Experiments/PerformanceStudy/catalina_bigger_5_runs_test_including_slice_2013-10-22_v2.out");
	// private static final File SCANNEDFILE = new
	// File("/media/84F01919F0191352/Projects/2013/paper/paper-olap4ld-demo/Experiments/PerformanceStudy/catalina_bigger_5_runs_test_2013-10-20.out");

	private static final File SQLITEFILE = new File(
			"/home/benedikt/Workspaces/Git-Repositories/olap4ld/OLAP4LD-trunk/testresources/bottleneck.db");

	public static void main(String argv[]) throws ClassNotFoundException,
			SQLException, IOException {

		ExperimentRun run1 = new ExperimentRun(
				new Bottleneck_ExperimentSystem(),
				new LDCX_ParameterSetProvider(), 1, SQLITEFILE, null);
		run1.run();

	}
}
