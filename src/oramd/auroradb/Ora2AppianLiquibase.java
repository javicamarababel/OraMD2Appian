package oramd.auroradb;

import java.io.PrintStream;

import oramd.mdl.FK;
import oramd.mdl.OraModel;
import oramd.mdl.OraTable;

public class Ora2AppianLiquibase extends Ora2Appian {

	protected static String LIQUIBASE_CONTEXT="ecv-mapfre";
	
	public Ora2AppianLiquibase(OraModel model) {
		super(model);
	}
	
	/* @Override */
	public String convertType(String tipo, String attrTipo) {
		String tipoConv;
		if ("date".equalsIgnoreCase(tipo)) {
			tipoConv="datetime";
		}
		else
			tipoConv=super.convertType(tipo, attrTipo);
		return tipoConv;
	}
	
	/* @Override */
	public void volcarTabla(OraTable table,PrintStream out) {
		CambiosTablaParaAppian cambiosTabla=new CambiosTablaParaAppian(table);
		volcarCreateTable(out,table,cambiosTabla);
		volcarAutoIncrement(out,table);
		volcarIndexes(out,table, cambiosTabla);
		
		String tableQName=getTableQName(table);
		aniadirFKs(tableQName,cambiosTabla); // Se volcarán luego, una vez creadas todas las tablas
	}

	protected void volcarInicioChangeset(PrintStream out,String label) {
		out.println("  <changeSet\r\n"
				+ "    id=\"TODO\"\r\n"
				+ "    author=\"babel\"\r\n"
				+ "    labels=\""+label+"\"\r\n"
				+ "    context=\""+LIQUIBASE_CONTEXT+"\""
				+ "  >");
	}
	
	protected void volcarFinChangeset(PrintStream out) {
		out.println("  </changeSet>\n");
	}
	
	/* @Override */
	public String getTipoPkSeq() {
		return "int";
	}
	
	protected void volcarColumna(PrintStream out,String nombreCol,String tipoDatos,String comentarios,boolean nullable,String otrasConstraints,String otrosAttrsCol) {
		out.print("      <column name=\""+nombreCol+"\" type=\""+tipoDatos+"\"");
		if (otrosAttrsCol!=null) {
			out.print(" ");
			out.print(otrosAttrsCol);
		}
		if (comentarios!=null) {
			out.print(" remarks=\""+comentarios+"\"");
		}
		out.println(">");
		out.println("        <constraints nullable=\""+nullable+"\" " + (otrasConstraints==null?"":otrasConstraints)+"/>");
		out.println("      </column>");
	}

	@Override
	protected void volcarColumnasNuevasFK(PrintStream out,OraTable table,CambiosTablaParaAppian cambiosTabla) {
		for (OraTable.OraColumn colFK:cambiosTabla.nuevasColsFk) {
			volcarColumna(out,colFK.getName(),getTipoPkSeq(),colFK.getComentario(),colFK.getNullable(),null,null);
	  }
	}

	public void printTableCol(OraTable.OraColumn col,PrintStream out,boolean unique) {
		String nombreCol=normalizaNombre(col.getName());
		String tipoAurora=determinarTipoAppian(nombreCol,col);
		String defaultValueAttr=null;
		String defaultValueComputed=determinarDefaultValueComputed(nombreCol,col);
		if (defaultValueComputed!=null) {
			defaultValueAttr="defaultValueComputed=\""+defaultValueComputed+"\"";
		}
		else {
			String defaultValue=determinarDefaultValue(nombreCol,col);
			if (defaultValue!=null) {
				defaultValueAttr="defaultValue=\""+defaultValue+"\"";
			}
		}
		volcarColumna(out,nombreCol,tipoAurora,col.getComentario(),col.getNullable(),unique?"unique=\"true\"":null,defaultValueAttr);
	}
	
	@Override
	protected void volcarColumnasTabla(PrintStream out,OraTable table,CambiosTablaParaAppian cambiosTabla) {
		String colUnicaPK="";
		if (table.getPkColNames().size()==1) {
			colUnicaPK=table.getPkColNames().iterator().next();
		}
		// Columnas Oracle convertidas
		for (OraTable.OraColumn col:table.getColumns()) {
			String comentarioCol=cambiosTabla.columnasComentadas.get(col.getName());
			if (comentarioCol==null) {
				printTableCol(col,out,colUnicaPK.equalsIgnoreCase(col.getName()));
			}
			else { // Columna comentada
				out.print("<!--");
				out.println(comentarioCol);
				printTableCol(col,out);
				out.println("-->");
			}
		}
	}
	
	protected void volcarCreateTable(PrintStream out,OraTable table,CambiosTablaParaAppian cambiosTabla) {
		String tableName=normalizaNombre(table.getName());
		volcarInicioChangeset(out,tableName);
		
		out.println("    <createTable tableName=\""+tableName+"\" schemaName=\""+schemaName+"\"");
		if (table.getComment()!=null) {
			out.print("       remarks=\""+corregirEncoding(table.getComment())+"\"");
		}
		out.println(">");

		// PK para Appian
		String seqPkName=getSeqPkForTable(tableName);
		volcarColumna(out,seqPkName,getTipoPkSeq(),"Clave secuencial autogenerada",false,"primaryKey=\"true\"",null);
		
		// Añadimos columnas por FK
		volcarColumnasNuevasFK(out,table,cambiosTabla);
		
		// Columnas Oracle convertidas
		volcarColumnasTabla(out,table,cambiosTabla);
		out.println("    </createTable>");
	
		volcarFinChangeset(out);
	}
	
	protected void volcarIndex(PrintStream out, String tableName, String indexName, String[] colNames) {
		volcarInicioChangeset(out, tableName);
		out.println("    <createIndex tableName=\""+tableName+"\" schemaName=\""+this.schemaName+"\"");
		out.println("	    indexName=\""+indexName+"\" >");
		for (String colName:colNames) {
			out.println("		<column name=\""+colName+"\"/>");
		}
		out.println("    </createIndex>");
		out.println("    <rollback>select true /* No sé cómo hacer rollback de esto ni me interesa demasiado */</rollback>");
		volcarFinChangeset(out);
	}
	
	protected void volcarIndexes(PrintStream out,OraTable tabla, CambiosTablaParaAppian cambiosTabla) {
		String tableName=normalizaNombre(tabla.getName());
		int numCols;
		if ((numCols=tabla.getPkColNames().size())>1) {
			// Volcamos PK compuesta como index
			String uniqueKeyName=getSecondaryKeyName(tabla);
			String[] cols=new String[numCols];
			int i=0;
			for (String colOriginal:tabla.getPkColNames()) {
				String colSustituidaPor=cambiosTabla.columnasSustituidas.get(colOriginal);
				String nombreCol;
				if (colSustituidaPor!=null)
					nombreCol=colSustituidaPor;
				else
					nombreCol=normalizaNombre(colOriginal);
				cols[i++]=nombreCol;
			}
			volcarIndex(out,tableName,uniqueKeyName,cols);
		}
		// Generamos 1 índice por cada columna con FK a otra tabla, para que la comprobación de integridad referencial 
		for (OraTable.OraColumn colFK:cambiosTabla.nuevasColsFk) {
			String colName=colFK.getName();
			volcarIndex(out,tableName,composeIndexName(tableName,colName),new String[] {colName});
		}
	}
	
	protected void volcarAutoIncrement(PrintStream out,OraTable table) {
		String tableName=normalizaNombre(table.getName());
		volcarInicioChangeset(out,tableName);
		out.println("    <addAutoIncrement tableName=\""+tableName+"\" schemaName=\""+this.schemaName+"\"");
		out.println("      columnName=\""+getSeqPkForTable(tableName)+"\"");
		out.println("      generationType=\"ALWAYS\"");
		out.println("    />");
		
		volcarFinChangeset(out);
	}


	@Override
	public void printNombreCol(String nombreCol,PrintStream out) {
		out.print("\"");
		out.print(nombreCol);
		out.print("\"");
	}
	
	public String determinarDefaultValue(String nombreCol,OraTable.OraColumn col) {
		String defaultValue=null;
		if ("mca_inh".equals(nombreCol)) {
			defaultValue="false";
		}
		return defaultValue;
	}
	
	public String determinarDefaultValueComputed(String nombreCol,OraTable.OraColumn col) {
		String defaultValue=null;
		if ("fec_actu".equals(nombreCol)) {
			defaultValue="NOW()";
		}
		return defaultValue;
	}
	
	protected static void volcarInicioChangelog(PrintStream out) {
		out.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n"
				+ "<databaseChangeLog\r\n"
				+ "        xmlns=\"http://www.liquibase.org/xml/ns/dbchangelog\"\r\n"
				+ "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\r\n"
				+ "        xmlns:pro=\"http://www.liquibase.org/xml/ns/pro\"\r\n"
				+ "        xsi:schemaLocation=\"http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd\r\n"
				+ "    http://www.liquibase.org/xml/ns/pro http://www.liquibase.org/xml/ns/pro/liquibase-pro-latest.xsd \">\r\n"
		);
	}
	
	@Override
	public void volcarAppianModel(PrintStream out) {
		volcarInicioChangelog(out);
		for (OraTable table:model.getTables()) {
			volcarTabla(table,out);
		}
		volcarFKs(out);
		volcarFinChangelog(out);
	}

	@Override
	protected void volcarFKs(PrintStream out) {
		for (OraTable tabla:tablasConFKs) {
			for (FK fk:tabla.getFKs()) {
				String tableName=getTableNameSinSchema(tabla.getName());
				volcarInicioChangeset(out, tableName);
				out.println("    <addForeignKeyConstraint baseTableName=\""+tableName+"\" baseTableSchemaName=\""+this.schemaName+"\"");
				out.println("	   baseColumnNames=\""+getListaCols(fk.getSrcCols())+"\"");
				out.println("	   constraintName=\""+composeFkName(tableName,fk)+"\"");
				out.println("	   onDelete=\"RESTRICT\"");
				out.println("	   referencedColumnNames=\""+getListaCols(fk.getDestCols())+"\"");
				out.println("	   referencedTableName=\""+fk.getDestTableName()+"\"");
				out.println("    />");
				out.println("    <rollback>select true /* No sé cómo hacer rollback de esto ni me interesa demasiado */</rollback>");
				volcarFinChangeset(out);
			}
		}
	}


	protected static void volcarFinChangelog(PrintStream out) {
		out.println("</databaseChangeLog>");
	}
	
	public static void main(String[] args) throws Exception {
		String sqlDeveloperExportFile=args[0];
		OraModel model=parseOraFile(sqlDeveloperExportFile);
		new Ora2AppianLiquibase(model).volcarAppianModel(System.out);
	}
}
