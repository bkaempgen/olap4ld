package org.olap4j.driver.olap4ld.test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
 * Example for exrunner used for LDCX performance evaluation.
 * @author günter ladwig
 *
 */
public class SortExperiments {

	public static class SortAttributes {
		public static final Attribute SIZE = new IntAttribute("size");
		public static final Attribute DIST = new StringAttribute("dist");
		public static final Attribute ALGORITHM = new StringAttribute("algorithm");
		public static final Attribute TIME = new IntAttribute("time");
	}
	
	public static class SortParameterSetProvider extends ParameterSetProvider {

		public SortParameterSetProvider() {
			registerAttributes(SortAttributes.SIZE, SortAttributes.DIST);
		}
		
		@Override
		public List<ParameterSet> getParameterSets() {
			List<ParameterSet> params = Lists.newArrayList();
			params.add(new SortParameterSet(512, "rand"));
			params.add(new SortParameterSet(1024, "rand"));
			params.add(new SortParameterSet(2048, "rand"));
			params.add(new SortParameterSet(4096, "rand"));
			params.add(new SortParameterSet(8192, "rand"));
			params.add(new SortParameterSet(16384, "rand"));
			params.add(new SortParameterSet(32768, "rand"));
			return params;
		}
	}
	
	public static class SortParameterSet extends ParameterSet {
		public SortParameterSet(int size, String dist) {
			m_attrValues.put(SortAttributes.SIZE, size);
			m_attrValues.put(SortAttributes.DIST, dist);
		}
		
		public int[] getData() {
			int[] data = new int[(Integer)getValue(SortAttributes.SIZE)];
			String dist = (String)getValue(SortAttributes.DIST);
			if ("rand".equals(dist)) {
				Random r = new Random(0);
				for (int i = 0; i < data.length; i++)
					data[i] = r.nextInt();
			}
			return data;
		}
	}
	
	public static class BubbleSortSystem extends ExperimentSystem {

		public BubbleSortSystem() {
			super("bubble");
			registerSystemAttribute(SortAttributes.ALGORITHM);
			registerExecutionAttribute(SortAttributes.TIME);
		}
		
		@Override
		public Map<Attribute,Object> open() throws IOException {
			return ImmutableMap.<Attribute,Object>of(SortAttributes.ALGORITHM, "bubble");
		}

		@Override
		public void close() throws IOException {
		}
		
		public static void sort(int[] x) {
			boolean unsorted = true;
			int temp;

			while (unsorted) {
				unsorted = false;
				for (int i = 0; i < x.length - 1; i++)
					if (x[i] > x[i + 1]) {
						temp = x[i];
						x[i] = x[i + 1];
						x[i + 1] = temp;
						unsorted = true;
					}
			}
		}

		@Override
		public Map<Attribute,Object> execute(ParameterSet query) throws IOException {
			int[] data = ((SortParameterSet)query).getData();
			
			long time = System.currentTimeMillis();
			sort(data);
			time = System.currentTimeMillis() - time;
			
			return ImmutableMap.<Attribute,Object>of(SortAttributes.TIME, time);
		}
		
	}
	
	public static class JavaSortSystem extends ExperimentSystem {

		public JavaSortSystem() {
			super("java");
			registerSystemAttribute(SortAttributes.ALGORITHM);
			registerExecutionAttribute(SortAttributes.TIME);
		}
		
		@Override
		public Map<Attribute,Object> open() throws IOException {
			return ImmutableMap.<Attribute,Object>of(SortAttributes.ALGORITHM, "java");
		}

		@Override
		public void close() throws IOException {
		}
		
		@Override
		public Map<Attribute,Object> execute(ParameterSet query) throws IOException {
			int[] data = ((SortParameterSet)query).getData();
			
			long time = System.currentTimeMillis();
			Arrays.sort(data);
			time = System.currentTimeMillis() - time;
			
			return ImmutableMap.<Attribute,Object>of(SortAttributes.TIME, time);
		}
		
	}
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		ExperimentRun run1 = new ExperimentRun(new BubbleSortSystem(), new SortParameterSetProvider(), 2, new File("sort.db"), null);
		run1.run();
		
		run1 = new ExperimentRun(new JavaSortSystem(), new SortParameterSetProvider(), 2, new File("sort.db"), null);
		run1.run();
	}
	
}
