package oramd.auroradb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oramd.mdl.FK;
import oramd.mdl.OraModel;
import oramd.mdl.OraTable;
import oramd.sqldeveloper.ParseMdlFromSQLDeveloperExport;
import java.util.List;
import java.util.ArrayList;

public class Ora2AppianMySQL extends Ora2Appian {

	public Ora2AppianMySQL(OraModel model) {
		super(model);
	}

	/* @Override */
	public String getTipoPkSeq() {
		return "int(11)";
	}
	
	/* @Override */
	public void volcarTabla(OraTable table,PrintStream out) {
		String tableName=normalizaNombre(table.getName());
		out.print("CREATE OR REPLACE TABLE `");
		out.print(tableName);
		out.print("` (\n");
		
		CambiosTablaParaAppian cambiosTabla=new CambiosTablaParaAppian(table);
		
		// PK para Appian
		String seqPkName=getSeqPkForTable(tableName);
		out.print(INDENT);
		out.print("`"+seqPkName+"` ");
		out.print(getTipoPkSeq());
		out.print(" NOT NULL AUTO_INCREMENT,\n");
		
		// Añadimos columnas por FK
		for (OraTable.OraColumn colFK:cambiosTabla.nuevasColsFk) {
			out.print(INDENT);
			printTableCol(out,colFK.getName(),getTipoPkSeq(),null,colFK.getNullable(),colFK.getComentario());
			out.print(",\n");
		}
		
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
					out.print("`");
					out.print(nombreCol);
					out.print("`");
					columnaUniqueKeyVolcada.add(nombreCol);
				}
			}
			if (columnaUniqueKeyVolcada.size()==1)
				columnaMonoKeyVolcada.addAll(columnaUniqueKeyVolcada);
			out.print(")");
		}

		// Volcar las columnas de FKs como KEY, para que tengan un índice 
		for (OraTable.OraColumn colFK:cambiosTabla.nuevasColsFk) {
			String columnaMonoKey=colFK.getName();
			if (!columnaMonoKeyVolcada.contains(columnaMonoKey)) {
				out.print(",\n");
				out.print(INDENT);
				out.print("KEY `");
				out.print(composeKeyNameForFkColumn(tableName,colFK.getName()));
				out.print("` (`");
				out.print(columnaMonoKey);
				out.print("`)");
				columnaMonoKeyVolcada.add(columnaMonoKey);
			}
		}

		out.print("\n)");
		if (table.getComment()!=null) {
			out.print(" COMMENT=\"");
			out.print(corregirEncoding(table.getComment()));
			out.print("\"");
		}
		out.print(";\n");
		
		aniadirFKs(tableName,cambiosTabla); // Se volcarán luego, una vez creadas todas las tablas
	}
	
	public void printTableCol(PrintStream out,String nombreCol,String tipo,String defaultValue,boolean isNullable,String comentario) {
	}
	
    /* @Override */
	public String determinarDefaultValue(String nombreCol,OraTable.OraColumn col) {
		String defaultValue=null;
		if ("fec_actu".equals(nombreCol)) {
			defaultValue="CURRENT_TIMESTAMP()";
		}
		else if ("mca_inh".equals(nombreCol)) {
			defaultValue="false";
		}
		return defaultValue;
	}
	
	@Override
	public void printTableCol(OraTable.OraColumn col,PrintStream out) {
		super.printTableCol(col,out);
		if (col.getComentario()!=null) {
			out.print(" COMMENT '");
			out.print(corregirEncoding(col.getComentario()));
			out.print("'");
		}
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

	public static void main(String[] args) throws Exception {
		String sqlDeveloperExportFile=args[0];
		//BufferedReader in=new BufferedReader(new FileReader(sqlDeveloperExportFile));
		BufferedReader in=new BufferedReader(new InputStreamReader(new FileInputStream(sqlDeveloperExportFile),"UTF-8"));
		OraModel model=(new ParseMdlFromSQLDeveloperExport()).parse(in,sqlDeveloperExportFile);
		new Ora2AppianMySQL(model).volcarAppianModel(System.out);
	}

}
