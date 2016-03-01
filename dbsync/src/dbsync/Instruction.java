package dbsync;

public class Instruction {
	private String toDo;
	private String toUndo;

	public Instruction(String toDo, String toUndo) {
		super();
		this.toDo = toDo;
		this.toUndo = toUndo;
	}

	public String getToDo() {
		return toDo;
	}

	public void setToDo(String toDo) {
		this.toDo = toDo;
	}

	public String getToUndo() {
		return toUndo;
	}

	public void setToUndo(String toUndo) {
		this.toUndo = toUndo;
	}

}
