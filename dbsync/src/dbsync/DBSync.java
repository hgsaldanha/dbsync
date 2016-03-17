package dbsync;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import dbsync.base.Database;
import dbsync.base.Field;
import dbsync.base.Table;
import dbsync.exception.DBSyncDatabaseException;

public class DBSync {
	private LocalDateTime date;
	private String user;
	private STATUS status;
	private Database source;
	private Database target;
	private List<Instruction> instructions;
	final String LOGTABLE = "dbsync_log";

	public enum STATUS {
		INICIADO("Iniciado"), CARREGADO("Carregado"), PREPARADO("Preparado"), APLICADO("Aplicado");
	
		private String nome;
	
		STATUS(String nome) {
			this.nome = nome;
		}
	
		public String toString() {
			return this.nome;
		}
	}

	public DBSync(String user, Database source, Database target) {
		this.date = LocalDateTime.now();
		this.user = user;
		this.instructions = new ArrayList<Instruction>();
		this.source = source;
		this.target = target;
		if (source.getConn() != null && target.getConn() != null)
			this.status = STATUS.INICIADO;
	}

	public LocalDateTime getDate() {
		return date;
	}

	public String getUser() {
		return user;
	}

	public STATUS getStatus() {
		return status;
	}

	public String getInstructions() throws DBSyncDatabaseException {
		if (!status.equals(STATUS.PREPARADO)) {
			throw new DBSyncDatabaseException("Instruções não preparadas.");
		}
		if (instructions.isEmpty())
			return "//NO UPDATES";
		StringBuilder inst = new StringBuilder();
		inst.append("//TO DO\n");
		for (Instruction instruction : instructions) {
			inst.append(instruction.getToDo()).append("\n");
		}
		inst.append("//TO UNDO\n");
		for (Instruction instruction : instructions) {
			inst.append(instruction.getToUndo()).append("\n");
		}
		return inst.toString();
	}

	public void load() throws DBSyncDatabaseException {
		if (status.equals(STATUS.INICIADO)) {
			try {
				source.load();
				target.load();
				status = STATUS.CARREGADO;
			} catch (DBSyncDatabaseException e) {
				throw e;
			}
		} else
			throw new DBSyncDatabaseException("DBSync não iniciado.");

	}

	public void preview() throws DBSyncDatabaseException {
		if (status.equals(STATUS.CARREGADO)) {
			try {
				// VERIFICA SE HÁ TABELAS PARA CRIAR
				for (Table table : source.getTables()) {
					if (!table.getName().equals(LOGTABLE))
						if (!target.getTables().contains(table))
							instructions.add(target.createTable(table));
				}
				// VERIFICA ALTERAÇÕES EM TABELAS
				for (Table targetTable : target.getTables()) {
					if (!targetTable.getName().equals(LOGTABLE)) {
						if (source.getTables().contains(targetTable)) {
							Table sourceTable = source.getTable(targetTable);
							for (Field sourceField : sourceTable.getFields()) {
								// CAMPOS PARA CRIAR
								if (!targetTable.getFields().contains(sourceField))
									instructions.add(target.alterTableAdd(targetTable, sourceField));
								// CAMPOS PARA ALTERAR
								if (targetTable.getFields().contains(sourceField)) {
									Field targetField = targetTable.getField(sourceField);
									if (!targetField.getType().equals(sourceField.getType()))
										instructions.add(target.alterTableModify(targetTable, sourceField, targetField));
								}
							}
							// CAMPOS PARA DROPAR
							for (Field targetField : targetTable.getFields()) {
								if (!sourceTable.getFields().contains(targetField))
									instructions.add(target.alterTableDrop(targetTable, targetField));
							}
						}
					}
				}
				// VERIFICA SE HÁ TABELAS PARA DROPAR
				for (Table table : target.getTables()) {
					if (!table.getName().equals(LOGTABLE))
						if (!source.getTables().contains(table))
							instructions.add(target.dropTable(table));
				}
			} catch (DBSyncDatabaseException e) {
				throw e;
			}
			status = STATUS.PREPARADO;
		} else
			throw new DBSyncDatabaseException("DBSync não carregado.");
	}

	public String synchronize() throws DBSyncDatabaseException {
		if (status.equals(STATUS.PREPARADO)) {
			if (!instructions.isEmpty()) {
				target.createLogTable(LOGTABLE);
				List<Instruction> logInstructions = new ArrayList<Instruction>();
				try (PreparedStatement stmt = target.getConn().prepareStatement(String.format("SELECT COUNT(*) AS total FROM `%s`", LOGTABLE))) {
					ResultSet rs = stmt.executeQuery();
					while (rs.next()) {
						if (rs.getInt("total") == 0) {
							for (Table table : source.getTables()) {
								logInstructions.add(source.createTable(table));
							}
						}
					}
					rs.close();
				} catch (SQLException e) {
					throw new DBSyncDatabaseException("Erro na preparação do log: " + e.getMessage());
				}
				
				try (Statement stmt = target.getConn().createStatement()) {
					target.getConn().setAutoCommit(false);
					for (Instruction instruction : instructions) {
						stmt.addBatch(instruction.getToDo());
					}
					StringBuilder toDos = new StringBuilder();
					StringBuilder toUndos = new StringBuilder();
					List<Instruction> writeInstructions = null;
					if (!logInstructions.isEmpty())
						writeInstructions = logInstructions;
					else 
						writeInstructions = instructions;
					for (Instruction instruction : writeInstructions) {
						toDos.append(instruction.getToDo()).append("\n");
						toUndos.append(instruction.getToUndo()).append("\n");
					}
					stmt.addBatch(String.format("INSERT INTO `%s` VALUES ('%s','%s','%s','%s')", LOGTABLE, this.getUser(), this.getDate(), toDos, toUndos));
					stmt.executeBatch();
					target.getConn().commit();
					return instructions.size() + " instruções executadas. Bancos de dados sincronizados.";
				} catch (SQLException e) {
					try {
						target.getConn().rollback();
					} catch (SQLException e1) {
						throw new DBSyncDatabaseException("Erro ao sincronizar bancos de dados: " + e.getMessage());
					}
					throw new DBSyncDatabaseException("Erro ao sincronizar bancos de dados: " + e.getMessage());
				}
			}
		} else
			throw new DBSyncDatabaseException("Os scripts ainda não foram preparados.");
		return "Nenhuma instrução para executar.";
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		target.getConn().close();
		source.getConn().close();
	}

}
