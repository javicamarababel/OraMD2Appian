package oramd.mdl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import oramd.util.Listas;

public class OraTable {

	protected String name;
	protected Map<String,OraColumn> columnas=new LinkedHashMap<String,OraColumn>();
	protected List<String> pkColNames=new ArrayList<String>();
	protected List<FK> fks=new ArrayList<FK>();
	protected String comment;
	
	public OraTable(String name) {
		this.name=name;
	}
	
	public String getName() {
		return name;
	}
	
	public String getComment() {
		return comment;
	}
	public void setComment(String comment) {
		this.comment=comment;
	}
	public void addNew(OraColumn col) throws OraModelException {
		String colName=col.getName();
		if (columnas.get(colName)!=null) {
			throw new OraModelException("La columna "+colName+" ya está en la tabla "+getName());
		}
		columnas.put(colName,col);
	}
	
	public Collection<OraColumn> getColumns() {
		return columnas.values();
	}
	
	public OraColumn getColumn(String colName) {
		return columnas.get(colName);
	}
	
	public void addPkCol(String colName) {
		if (getColumn(colName)==null)
			throw new IllegalArgumentException("La columna \""+colName+"\" no puede ser PK de la tabla \""+getName()+"\" porque no está en esa tabla");
		pkColNames.add(colName);
	}
	
	public boolean hayPk() {
		return pkColNames.size()>0;
	}
	public Collection<String> getPkColNames() {
		return pkColNames;
	}
	
	public String toString() {
		StringBuilder s=new StringBuilder("Tabla ");
		String INDENT="  ";
		
		s.append(name);
		s.append("\nColumnas:\n");
		s.append(Listas.collToString(columnas.values(),INDENT));
		if (hayFKs()) {
			s.append("\nFKs:\n");
			s.append(Listas.collToString(fks,INDENT));
		}
		
		s.append("\n");
		
		return s.toString();
	}
	
	protected void checkAllColsExist(String[] cols) {
		for (String col:cols) {
			if (columnas.get(col)==null) {
				throw new IllegalArgumentException("La columna \""+col+"\" no está en la tabla "+name);
			}
		}
	}
	
	public void addFK(String[] srcCols,String destTableName,String[] destCols) {
		checkAllColsExist(srcCols);
		fks.add(new FK(srcCols,destTableName,destCols));
	}
	
	public boolean hayFKs() {
		return fks.size()>0;
	}
	
	public List<FK> getFKs() {
		return fks;
	}
	
	public void setFKs(List<FK> fks) {
		this.fks=fks;
	}

	public class OraColumn {
		protected String nombreColumna,tipoColumna,attrTipoColumna;
		protected boolean nullable=true;
		protected String comentario;
		
		public OraColumn(String nombreColumna,String tipoColumna,String attrTipoColumna) {
			this.nombreColumna=nombreColumna;
			this.tipoColumna=tipoColumna;
			this.attrTipoColumna=attrTipoColumna;
		}

		public String getName() {
			return nombreColumna;
		}
		
		public String getTipo() {
			return tipoColumna;
		}
		
		public String getAttrTipo() {
			return attrTipoColumna;
		}
		
		public void setNullable(boolean nullable) {
			this.nullable=nullable;
		}
		
		public boolean getNullable() {
			return nullable;
		}
		
		public void setComentario(String comentario) {
			this.comentario=comentario;
		}
		
		public String getComentario() {
			return comentario;
		}
		
		public String toString() {
			StringBuilder s=new StringBuilder("Columna ");
			
			s.append(nombreColumna);
			s.append(" ");
			s.append(tipoColumna);
			if (attrTipoColumna!=null) {
				s.append(attrTipoColumna);
			}
			if (getNullable())
				s.append(" NULL");
			else
				s.append(" NOT NULL");
			if (comentario!=null) {
				s.append(" COMMENT '");
				s.append(comentario);
				s.append("'");
			}

			return s.toString();
		}
		
	} /* OraColumn */
	
} /* class OraTable */
