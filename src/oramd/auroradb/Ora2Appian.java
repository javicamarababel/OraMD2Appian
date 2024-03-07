package oramd.auroradb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oramd.mdl.FK;
import oramd.mdl.OraModel;
import oramd.mdl.OraTable;
import oramd.sqldeveloper.MdlParseException;
import oramd.sqldeveloper.ParseMdlFromSQLDeveloperExport;

public abstract class Ora2Appian {

	protected static final String INDENT="  ";
	protected static Pattern CHARLEN_ER=Pattern.compile("\\(([0-9]+).*");
	protected static Pattern TABLENAME_ER=Pattern.compile("..._(...)_(.+)");
	
	protected OraModel model;
	protected List<OraTable> tablasConFKs=new ArrayList<OraTable>();
	protected String schemaName="ecv_emisioncolectivovida";
	
	public Ora2Appian(OraModel model) {
		this.model=model;
	}
	
	public void volcarAppianModel(PrintStream out) {
		for (OraTable table:model.getTables()) {
			volcarTabla(table,out);
			out.print("\n");
		}
		volcarFKs(out);
	}
	
	public abstract void volcarTabla(OraTable table,PrintStream out);
	
	protected String getTableQName(String tableName) {
		return schemaName+"."+normalizaNombre(tableName);
	}
	protected String getTableQName(OraTable table) {
		return getTableQName(table.getName());
	}
	protected String getTableNameSinSchema(String tableQName) {
		int pos=tableQName.indexOf('.');
		return pos>0?tableQName.substring(pos+1):tableQName;
	}

	protected void volcarFKs(PrintStream out) {
		for (OraTable tabla:tablasConFKs) {
			out.print("ALTER TABLE `");
			out.print(getTableQName(tabla));
			out.print("`");
			
			boolean primFk=true;
			for (FK fk:tabla.getFKs()) {
				if (primFk) {
					primFk=false;
				}
				else {
					out.print(",");
				}
				out.print("\n");
				out.print(INDENT);
				String fkName=composeFkName(tabla.getName(),fk);
				out.print("ADD CONSTRAINT ");
				out.print(fkName);
				out.print(" FOREIGN KEY (");
				printListaCols(out,fk.getSrcCols());
				out.print(") REFERENCES ");
				String destTableName=getTableQName(fk.getDestTableName());
				out.print(destTableName);
				out.print(" (");
				printListaCols(out,fk.getDestCols());
				out.print(")");
			}
			out.print(";\n");
			//out.print("COMMIT;\n");
		}
	}


	public static String corregirEncoding(String s) {
		return s.replace("µ", "Á")
				.replace("Ã?", "Á")
				.replace("Ã?", "Í")
				.replace("Ö", "Í")
				.replace("Ã–", "Í")
				.replace("Ã“","Ó")
				.replace("Ãš","Ú")
				.replace("¥", "Ñ");
	}
	
	public static String normalizaNombre(String n) {
		return n.toLowerCase();
	}
	
	public static String composeFkName(String srcTableName,FK fk) {
		StringBuilder fkName=new StringBuilder(srcTableName);
		fkName.append("_FK");
		for (String col:fk.getSrcCols()) {
			fkName.append("_");
			fkName.append(col);
		}
		return fkName.toString();
	}
	
	public static String composeKeyNameForFkColumn(String tableName,String fkColName) {
		return tableName+"_"+fkColName; 
	}
	
	protected static void printListaCols(PrintStream out,String[] cols) {
		boolean primCol=true;
		for (String col:cols) {
			if (primCol) {
				primCol=false;
			}
			else {
				out.print(',');
			}
			out.print("`");
			out.print(col);
			out.print("`");
		}
	}
	
	protected void aniadirFKs(String nombreTabla,CambiosTablaParaAppian cambios) {
		if (cambios.fks.size()>0) {
			OraTable table=new OraTable(nombreTabla);
			table.setFKs(cambios.fks);
			tablasConFKs.add(table);
		}
	}
	
	public static String getSecondaryKeyName(OraTable table) {
		return normalizaNombre(table.getName()+"_FUNCTIONAL_KEY");
	}
	
	public static String getSeqPkForTable(String tableName) {
		Matcher m;
		String seqPk=null;
		if ((m=TABLENAME_ER.matcher(tableName)).matches()) {
			//String tipoTabla=m.group(1);
			String nombreTabla=m.group(2);
			seqPk="ID_SEQ_"+nombreTabla;
		}
		return normalizaNombre(seqPk);
	}
	
	protected static int getCharLen(String attrTipo) {
		int charLen=0;
		Matcher m;
		if ((m=CHARLEN_ER.matcher(attrTipo)).matches()) {
			charLen=Integer.parseInt(m.group(1));
		}
		return charLen;
	}
	
	public void printTableCol(PrintStream out,String nombreCol,String tipo,String defaultValue,boolean isNullable) {
		printNombreCol(nombreCol,out);
		out.print(tipo);
		if (isNullable)
			out.print(" NULL");
		else
			out.print(" NOT NULL");
		if (defaultValue!=null) {
			out.print(" DEFAULT ");
			out.print(defaultValue);
		}
	}
	public void printNombreCol(String nombreCol,PrintStream out) {
		out.print(nombreCol);
	}

	protected void volcarColumnasTabla(PrintStream out,OraTable table,CambiosTablaParaAppian cambiosTabla) {
		// Columnas Oracle convertidas
		for (OraTable.OraColumn col:table.getColumns()) {
			out.print(INDENT);
			String comentarioCol=cambiosTabla.columnasComentadas.get(col.getName());
			if (comentarioCol==null) {
				printTableCol(col,out);
				out.print(",");
			}
			else { // Columna comentada
				out.print("/* ");
				printTableCol(col,out);
				out.print(" --- ");
				out.print(comentarioCol);
				out.print("*/");
			}
			out.print("\n");
		}
	}
	public void printTableCol(OraTable.OraColumn col,PrintStream out) {
		String nombreCol=normalizaNombre(col.getName());
		String tipoAurora=determinarTipoAppian(nombreCol,col);
		String defaultValue=determinarDefaultValue(nombreCol,col);
		printTableCol(out,nombreCol,tipoAurora,defaultValue,col.getNullable());
	}

	public String determinarTipoAppian(String nombreCol,OraTable.OraColumn col) {
		String tipoAurora;
		
		if (nombreCol.startsWith("cod_usr"))
			tipoAurora="varchar(64)";
		else {
			tipoAurora=convertType(col.getTipo(),col.getAttrTipo());
			if ((nombreCol.startsWith("mca_") || nombreCol.startsWith("val_")) && tipoAurora.equals("varchar(1)"))
				tipoAurora="boolean";
		}
		return tipoAurora;
	}

	public abstract String determinarDefaultValue(String nombreCol,OraTable.OraColumn col);
	public String convertType(String tipo, String attrTipo) {
		String tipoConv;
		if ("varchar2".equalsIgnoreCase(tipo)) {
			int len=getCharLen(attrTipo);
			tipoConv="varchar("+len+")";
		}
		else if ("number".equalsIgnoreCase(tipo)) {
			if (attrTipo==null)
				attrTipo="(10)";
			tipoConv="decimal"+attrTipo;
		}
		else if ("timestamp".equalsIgnoreCase(tipo)) {
			tipoConv="datetime";
		}
		else if ("clob".equalsIgnoreCase(tipo)) {
			tipoConv="text";
		}
		else
			throw new IllegalArgumentException("No conozco el tipo Oracle "+tipo+attrTipo);
		return tipoConv;
	}

	protected void volcarColumnasNuevasFK(PrintStream out,OraTable table,CambiosTablaParaAppian cambiosTabla) {
		for (OraTable.OraColumn colFK:cambiosTabla.nuevasColsFk) {
			out.print(INDENT);
			printTableCol(out,colFK.getName(),getTipoPkSeq(),null,colFK.getNullable());
			out.print(",\n");
	  }
	}

	public abstract String getTipoPkSeq();

	protected void volcarPK(PrintStream out,String seqPkName,OraTable table,CambiosTablaParaAppian cambiosTabla) {

		// PK
		out.print(INDENT+"PRIMARY KEY (`"+seqPkName+"`)");
	
		Set<String> columnaMonoKeyVolcada=new HashSet<String>(); // Evitamos volcar claves monocolumna con las mismas columnas
		// Volcar la PK de Oracle como UNIQUE KEY
		if (table.hayPk()) {
			out.print(",\n");
			out.print(INDENT);
			out.print("UNIQUE KEY `");
			String uniqueKeyName=getSecondaryKeyName(table);
			out.print(uniqueKeyName);
			out.print("` (");
			Set<String> columnaUniqueKeyVolcada=new HashSet<String>();
			boolean firstCol=true;
			for (String pkCol:table.getPkColNames()) {
				String nombreCol;
				String colSustituidaPor=cambiosTabla.columnasSustituidas.get(pkCol);
				if (colSustituidaPor!=null)
					nombreCol=colSustituidaPor;
				else
					nombreCol=normalizaNombre(pkCol);
				if (!columnaUniqueKeyVolcada.contains(nombreCol)) {
					if (firstCol)
						firstCol=false;
					else
						out.print(",");
					printNombreCol(nombreCol,out);
					columnaUniqueKeyVolcada.add(nombreCol);
				}
			}
			if (columnaUniqueKeyVolcada.size()==1)
				columnaMonoKeyVolcada.addAll(columnaUniqueKeyVolcada);
			out.print(")");
		}
		
		// Volcar las columnas de FKs como KEY, para que tengan un índice 
		String tableName=normalizaNombre(table.getName());
		for (OraTable.OraColumn colFK:cambiosTabla.nuevasColsFk) {
			String columnaMonoKey=colFK.getName();
			if (!columnaMonoKeyVolcada.contains(columnaMonoKey)) {
				out.print(",\n");
				out.print(INDENT);
				out.print("KEY ");
				out.print(composeKeyNameForFkColumn(tableName,colFK.getName()));
				out.print("` (");
				out.print(columnaMonoKey);
				out.print(")");
				columnaMonoKeyVolcada.add(columnaMonoKey);
			}
		}		
	}

	protected static OraModel parseOraFile(String sqlDeveloperExportFile) throws IOException, MdlParseException {
		BufferedReader in=new BufferedReader(new InputStreamReader(new FileInputStream(sqlDeveloperExportFile),"UTF-8"));
		OraModel model=(new ParseMdlFromSQLDeveloperExport()).parse(in,sqlDeveloperExportFile);
		return model;
	}
	
	protected static String getListaCols(String[] cols) {
		StringBuilder lista=new StringBuilder();
		for (String col:cols) {
			if (lista.length()>0) {
				lista.append(",");
			}
			lista.append(col);
		}
		return lista.toString();
	}
	
	protected String composeIndexName(String tabla,String columna) {
		return tabla+"_"+columna;
	}
}
