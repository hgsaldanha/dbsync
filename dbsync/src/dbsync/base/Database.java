package dbsync.base;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import dbsync.Instruction;
import dbsync.exception.DBSyncDatabaseException;

public abstract class Database {

	private Connection conn = null;
	private String host;
	private String user;
	private String passwd;
	private String schema;
	private List<Table> tables = null;

	public Database(String driver) throws DBSyncDatabaseException {
		loadDriver(driver);
	}

	public Database(String driver, String host, String schema, String user, String pass)
			throws DBSyncDatabaseException {
		loadDriver(driver);
		this.host = host;
		this.schema = schema;
		this.user = user;
		this.passwd = pass;
		this.connect();
	}

	private void loadDriver(String driver) throws DBSyncDatabaseException {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			throw new DBSyncDatabaseException("Erro ao carregar driver JDBC - " + driver);
		}
	}
	
	public Table getTable(Table tbl) {
		for (Table table : getTables()) {
			if (table.equals(tbl))
				return table;
		}
		return null;
	}

	public abstract void connect() throws DBSyncDatabaseException;

	public abstract void load() throws DBSyncDatabaseException;

	public abstract Instruction createTable(Table table) throws DBSyncDatabaseException;

	public abstract Instruction alterTableAdd(Table table, Field field) throws DBSyncDatabaseException;

	public abstract Instruction alterTableModify(Table table, Field from, Field to) throws DBSyncDatabaseException;

	public abstract Instruction alterTableDrop(Table table, Field field) throws DBSyncDatabaseException;

	public abstract Instruction dropTable(Table table) throws DBSyncDatabaseException;
	
	public abstract void createLogTable(String name) throws DBSyncDatabaseException;

	public Connection getConn() {
		return conn;
	}

	public String getHost() {
		return host;
	}

	public String getUser() {
		return user;
	}

	public String getPasswd() {
		return passwd;
	}

	public List<Table> getTables() {
		if (tables == null)
			tables = new ArrayList<Table>();
		return tables;
	}

	public String getSchema() {
		return schema;
	}

	public void setTables(List<Table> tables) {
		this.tables = tables;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

}
