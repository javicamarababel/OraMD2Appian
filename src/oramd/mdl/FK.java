package oramd.mdl;

import oramd.util.Listas;

public class FK {
	protected String[] srcCols;
	protected String destTableName;
	protected String[] destCols;
	
	public FK(String[] srcCols,String destTableName,String[] destCols) {
		this.srcCols=srcCols;
		this.destTableName=destTableName;
		this.destCols=destCols;
	}
	
	public String toString() {
		StringBuilder s=new StringBuilder("FK desde columnas ");
		s.append(Listas.listaToString(srcCols));
		s.append(" hacia tabla ");
		s.append(destTableName);
		s.append(", columnas ");
		s.append(Listas.listaToString(destCols));
		return s.toString();
	}
	
	public String[] getSrcCols() {
		return srcCols;
	}
	
	public String[] getDestCols() {
		return destCols;
	}
	
	public String getDestTableName() {
		return destTableName;
	}
} /* FK */

