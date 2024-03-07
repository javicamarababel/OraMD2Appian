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

public class Ora2AppianPostgres extends Ora2Appian {

	public Ora2AppianPostgres(OraModel model) {
		super(model);
	}
	
	public String getTipoPkSeq() {
		return ""; /* Va implícito en el SERIAL */
	}
	
	/* @Override */
	public void volcarTabla(OraTable table,PrintStream out) {
		String tableQName=getTableQName(table);
		out.print("CREATE TABLE ");
		out.print(tableQName);
		out.print(" (\n");
		
		CambiosTablaParaAppian cambiosTabla=new CambiosTablaParaAppian(table);
		
		// PK para Appian
		String seqPkName=getSeqPkForTable(table.getName());
		out.print(INDENT);
		out.print(seqPkName);
		out.print(" SERIAL,\n");
		
		// Añadimos columnas por FK
		volcarColumnasNuevasFK(out,table,cambiosTabla);
		
		// Columnas Oracle convertidas
		volcarColumnasTabla(out,table,cambiosTabla);
		
		volcarPK(out,seqPkName,table,cambiosTabla);

		out.print("\n)");
		
		// Comentarios de tabla
		out.print("\n");
		if (table.getComment()!=null) {
			out.print(" COMMENT ON TABLE ");
			out.print(tableQName);
			out.print(" IS '");
			out.print(corregirEncoding(table.getComment()));
			out.print("'");
		}
		out.print(";\n");
		
		// Comentarios de columnas
		for (OraTable.OraColumn col:table.getColumns()) {
			String colName=col.getName();
			if (cambiosTabla.columnasComentadas.get(colName)==null) { // Ignoramos columnas comented out
				String comentarioCol=col.getComentario();
				if (comentarioCol!=null) {
					out.print(" COMMENT ON COLUMN ");
					out.print(tableQName);
					out.print(".");
					out.print(colName);
					out.print(" IS '");
					out.print(corregirEncoding(comentarioCol));
					out.print("'");
				}
			}

		}
		
		aniadirFKs(tableQName,cambiosTabla); // Se volcarán luego, una vez creadas todas las tablas
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

	public void printNombreCol(String nombreCol,PrintStream out) {
		out.print("`");
		out.print(nombreCol);
		out.print("` ");
	}
	
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
	
	public static void main(String[] args) throws Exception {
		String sqlDeveloperExportFile=args[0];
		//BufferedReader in=new BufferedReader(new FileReader(sqlDeveloperExportFile));
		BufferedReader in=new BufferedReader(new InputStreamReader(new FileInputStream(sqlDeveloperExportFile),"UTF-8"));
		OraModel model=(new ParseMdlFromSQLDeveloperExport()).parse(in,sqlDeveloperExportFile);
		new Ora2AppianPostgres(model).volcarAppianModel(System.out);
	}
}
