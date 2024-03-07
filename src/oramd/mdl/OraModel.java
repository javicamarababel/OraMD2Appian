package oramd.mdl;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OraModel {

	protected static final Pattern TABLA_CON_SCHEMA_ER=Pattern.compile("[^.]*\\.([^ ]*)");
	
	public static String simplificarNombreTabla(String nombreTabla) {
		Matcher m;
		if ((m=TABLA_CON_SCHEMA_ER.matcher(nombreTabla)).matches()) {
			nombreTabla=m.group(1); // Nos quedamos con lo que haya a la derecha del .
		}
		nombreTabla=nombreTabla.replace("\"", ""); // Quitamos "
		return nombreTabla;
	}
	
	public static String simplificarNombreColumna(String nombreCol) {
		nombreCol=nombreCol.replace("\"", ""); // Quitamos "
		return nombreCol;
	}
	
	protected Map<String,OraTable> tables=new LinkedHashMap<String,OraTable>()			;
	
	public void addNewTable(OraTable t) throws OraModelException {
		String tableName=t.getName();
		if (tables.get(tableName)!=null) {
			throw new OraModelException("La tabla "+tableName+" ya está en el modelo");
		}
		tables.put(tableName,t);
	}
	
	public Collection<OraTable> getTables() {
		return tables.values();
	}
	
	public OraTable getTable(String tableName) {
		return tables.get(tableName);
	}
	
	public String toString() {
		StringBuilder s=new StringBuilder();
		
		for (OraTable t:tables.values()) {
			s.append(t.toString());
			s.append("\n");
		}
		return s.toString();
	}

	public OraTable getTableMandatory(String tableName,String context) {
		OraTable t=getTable(tableName);
		if (t==null)
			throw new NullPointerException("Tabla "+tableName+" no encontrada para \n"+context);
		return t;
	}

	public OraTable.OraColumn getTableColumnMandatory(String tableName,String colName,String context) {
		OraTable t=getTableMandatory(tableName,context);
		OraTable.OraColumn col=t.getColumn(colName);
		if (col==null)
			throw new NullPointerException("Columna "+colName+" de tabla "+tableName+" no encontrada para \n"+context);
		return col;
	}
}
