package org.olap4j.driver.ld;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.olap4j.OlapConnection;
import org.olap4j.OlapWrapper;

/**
 * This class
 * 
 * @author b-kaempgen
 * 
 */
public class MondrianOlapServerEngine {

	private OlapConnection myMondrianConnection;

	private String catalogfile;
	private String mysqldb;

	public MondrianOlapServerEngine(LdOlap4jDriver driver, String url, Properties info) {
		catalogfile = url;
		mysqldb = url;
	}

	/**
	 * We commit the xml changes to the catalog.
	 */
	private void writeToCatalog() {
		// Standard header
		// Shared dimensions (name)
		// Hierarchies (name, hasAll, primaryKey, table name)
		// Levels (name, column, unique members, type)
		// Cubes (name, table name)
		// Used Dimensions
		// Used Measures (name, column, aggregator, format string)
		// Multicube (name, ignore unrelated dimensions)
		// Cubes
		// Dimensions
		// Measures
	}

	/**
	 * Refresh server
	 */
	public void refresh() {
		;
	}

	/**
	 * Here, we create the Mondrian connection that we will use as back end.
	 * 
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public boolean connect() {

		try {
			Class.forName("mondrian.olap4j.MondrianOlap4jDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Connection connection;
		OlapConnection olapConnection = null;
		try {
			
			// TODO: Currently, connection setting of Mondrian OLAP server is done here, manually.
//			connection = DriverManager
//					.getConnection("jdbc:mondrian:Jdbc=jdbc:mysql://localhost/cubedb?user=foodmart&password=foodmart;Catalog=res:foodmart/cubefile.xml;JdbcDrivers=com.mysql.jdbc.Driver;");
			connection = DriverManager.getConnection(mysqldb);
			OlapWrapper wrapper = (OlapWrapper) connection;
			olapConnection = wrapper
					.unwrap(OlapConnection.class);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (olapConnection == null) {
			return false;
		} else {
			myMondrianConnection = olapConnection;
			return true;
		}
	}

	/**
	 * Is used by SLD driver for querying.
	 */
	public OlapConnection getOlapConnection() {
		// TODO: At the moment, the mondrian connection is simply done if the connection is asked for. 
		if (myMondrianConnection == null) {
			connect();
		}
		return myMondrianConnection;
	}
}
