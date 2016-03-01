package dbsync.loader;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import dbsync.Instruction;
import dbsync.base.Database;
import dbsync.base.Field;
import dbsync.base.Table;
import dbsync.exception.DBSyncDatabaseException;

public class MySQLDatabase extends Database {

	public MySQLDatabase() throws DBSyncDatabaseException {
		super("com.mysql.jdbc.Driver");
	}
	
	public MySQLDatabase(String host, String schema, String user, String passwd) throws DBSyncDatabaseException {
		super("com.mysql.jdbc.Driver", host, schema, user, passwd);
	}

	@Override
	public void connect() throws DBSyncDatabaseException {
		try {
			//"jdbc:mysql://localhost:3306/information_schema","root", "root"
			StringBuilder url = new StringBuilder();
			url.append("jdbc:mysql://").append(this.getHost()).append("/").append(this.getSchema());
			this.setConn(DriverManager.getConnection(url.toString(), this.getUser(), this.getPasswd()));
		} catch (SQLException e) {
			throw new DBSyncDatabaseException("Falha na conexão: " + e.getMessage());
		}
	}

	@Override
	public void load() throws DBSyncDatabaseException {
		List<Table> tables = null;
		List<Field> fields = null;
		String selectSQL = "SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?";
		if (this.getConn() != null) {
			try(PreparedStatement ps = this.getConn().prepareStatement(selectSQL))  {
				ps.setString(1, this.getSchema());
				ResultSet rs = ps.executeQuery();
				tables = new ArrayList<Table>();
				while (rs.next()) {
					Table table = new Table();
					table.setName(rs.getString("TABLE_NAME"));
					table.setType(rs.getString("ENGINE"));
					tables.add(table);
				}
			} catch (SQLException e) {
				throw new DBSyncDatabaseException("Erro ao tentar localizar as tabelas do Schema: " + e.getMessage());
			}
			if (tables != null && tables.size() > 0) {
				selectSQL = "SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
				try(PreparedStatement ps = this.getConn().prepareStatement(selectSQL))  {
					for (Table table : tables) {
						ps.setString(1, this.getSchema());
						ps.setString(2, table.getName());
						ResultSet rs = ps.executeQuery();
						fields = new ArrayList<Field>();
						while (rs.next()) {
							Field field = new Field();
							field.setName(rs.getString("COLUMN_NAME"));
							field.setType(rs.getString("COLUMN_TYPE"));
							field.setNullable(rs.getString("IS_NULLABLE").equals("YES")?true:false);
							fields.add(field);
						}
						table.setFields(fields);
					}
				} catch (SQLException e) {
					throw new DBSyncDatabaseException("Erro ao carregar os campos das tabelas: " + e.getMessage());
				}
				this.setTables(tables);
			}
		} else {
			throw new DBSyncDatabaseException("Conexão inválida.");
		}
	}

	@Override
	public Instruction createTable(Table table) throws DBSyncDatabaseException {
		StringBuilder sql = new StringBuilder("CREATE TABLE ").append(table.getName()).append(" (");
		for (Field field : table.getFields()) {
			sql.append("`").append(field.getName()).append("`").append(" ").append(field.getType()).append(" ").append(field.isNullableStr());
			sql.append(", ");
		}
		sql.deleteCharAt(sql.length()-2).append(");");
		
		return new Instruction(sql.toString(), String.format("DROP TABLE `%s`;", table.getName()));
	}


	@Override
	public Instruction alterTableAdd(Table table, Field field) throws DBSyncDatabaseException {
		String toDo = String.format("ALTER TABLE `%s` ADD COLUMN `%s` %s %s;", table.getName(), field.getName(), field.getType(), field.isNullableStr());
		String toUndo = String.format("ALTER TABLE `%s` DROP COLUMN `%s`;", table.getName(), field.getName());
		return new Instruction(toDo, toUndo);
	}

	@Override
	public Instruction alterTableModify(Table table, Field from, Field to) throws DBSyncDatabaseException {
		String toDo = String.format("ALTER TABLE `%1$s` CHANGE COLUMN `%2$s` `%2$s` %3$s %4$s;", table.getName(), from.getName(), from.getType(), from.isNullableStr());
		String toUndo = String.format("ALTER TABLE `%1$s` CHANGE COLUMN `%2$s` `%2$s` %3$s %4$s;", table.getName(), from.getName(), to.getType(), to.isNullableStr());
		return new Instruction(toDo, toUndo);
	}

	@Override
	public Instruction alterTableDrop(Table table, Field field) throws DBSyncDatabaseException {
		String toDo = String.format("ALTER TABLE `%s` DROP COLUMN `%s`;", table.getName(), field.getName());
		return new Instruction(toDo, alterTableAdd(table, field).getToDo());
	}

	@Override
	public Instruction dropTable(Table table) throws DBSyncDatabaseException {
		return new Instruction(String.format("DROP TABLE `%s`;", table.getName()), createTable(table).getToDo());
	}

	@Override
	public void createLogTable(String name) throws DBSyncDatabaseException {
		try (Statement stmt = this.getConn().createStatement()) {
			stmt.executeUpdate(String.format("CREATE TABLE IF NOT EXISTS `%s` (user VARCHAR(30), date_time DATETIME, todo LONGTEXT, toundo LONGTEXT);", name));
		} catch (SQLException e) {
			throw new DBSyncDatabaseException(e.getMessage());
		}
	}

}
