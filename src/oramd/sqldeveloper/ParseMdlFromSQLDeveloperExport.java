package oramd.sqldeveloper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oramd.mdl.OraModel;
import oramd.mdl.OraTable;

public class ParseMdlFromSQLDeveloperExport {
	
	protected static final Pattern CREATE_TABLE_ER=Pattern.compile("^.*CREATE TABLE ([^ ]+).*$");
	protected static final Pattern COLUMN_ER=Pattern.compile("^[^(\"]*\\(?[ \t]*([^ ]+) ([^ (,]+) *(\\([^)]+\\))?[^,]*,? *$");
	protected static final Pattern FIN_TABLA_ER=Pattern.compile("^ *\\).*$");
	//ALTER TABLE "ECV_LD"."ECV_DAT_AHORRO_EMI" MODIFY ("COD_EXPEDIENTE" NOT NULL ENABLE);
	protected static final Pattern NOT_NULLABLE_ER=Pattern.compile(" *ALTER TABLE [^.]+\\.([^ ]+) MODIFY *\\(([^ ]+) NOT NULL ENABLE\\).*;");
	//ALTER TABLE "ECV_LD"."ECV_DAT_AHORRO_EMI" ADD CONSTRAINT "PK_ECV_DAT_AHORRO_EMI" PRIMARY KEY ("COD_EXPEDIENTE")
	protected static final Pattern PK_ER=Pattern.compile(" *ALTER TABLE [^.]+\\.([^ ]+) ADD CONSTRAINT [^ ]+ PRIMARY KEY \\(([^)]+)\\).*");
	//COMMENT ON COLUMN "ECV_LD"."ECV_DAT_AHORRO_EMI"."COD_EXPEDIENTE" IS 'IDENTIFICADOR DEL EXPEDIENTE ASOCIADO A LA SOLICITUD';
	protected static final Pattern COMMENT_ER=Pattern.compile(" *COMMENT ON COLUMN [^.]+\\.([^ ]+)\\.([^ ]+) IS '([^']+)'.*");
	// ALTER TABLE "ECV_LD"."ECV_DAT_RIESGOS_EMI" ADD CONSTRAINT "FK_ECV_DAT_RIESGOS_EMI_FOR_COB" FOREIGN KEY ("COD_FORMA_COB")
	// REFERENCES "ECV_LD"."ECV_GEN_FORMA_COBRO" ("COD_FORMA_COB") ENABLE;
	protected static final Pattern FK1_ER=Pattern.compile("[ \t]*ALTER TABLE [^.]+\\.([^ ]+) ADD CONSTRAINT [^ ]+ FOREIGN KEY \\(([^)]+)\\).*");
	protected static final Pattern FK2_ER=Pattern.compile("[ \t]*REFERENCES [^.]+\\.([^ ]+) \\(([^)]+)\\).*");
	// COMMENT ON TABLE "ECV_LD"."ECV_GEN_TIP_ACCIONSR"  IS 'TABLA QUE ALMACENA LAS TIPOLOGIAS DE ACCIONES PARA LA SELECCIÓN DE RIESGOS';
	protected static final Pattern TABLECOMMENT_ER=Pattern.compile("[ \t]*COMMENT ON TABLE [^.]+\\.([^ ]+) *IS *'([^']+).*");

	public OraModel parse(BufferedReader in,String inputDescription) throws MdlParseException {
		try {
			OraModel model=new OraModel();
			String line;
			OraTable tablaActual=null;
			while ((line=in.readLine())!=null) {
				Matcher m;
				if (tablaActual==null) {
					if ((m=CREATE_TABLE_ER.matcher(line)).matches()) {
						String tableName=m.group(1);
						tableName=OraModel.simplificarNombreTabla(tableName);
						tablaActual=new OraTable(tableName);
						model.addNewTable(tablaActual);
					}
					else if ((m=NOT_NULLABLE_ER.matcher(line)).matches()) {
						String tableName=m.group(1);
						String colName=m.group(2);
						tableName=OraModel.simplificarNombreTabla(tableName);
						colName=OraModel.simplificarNombreColumna(colName);
						OraTable.OraColumn col=model.getTableColumnMandatory(tableName,colName,line);
						col.setNullable(false);
					}
					else if ((m=PK_ER.matcher(line)).matches()) {
						String tableName=m.group(1);
						String pkColsList=m.group(2);
						tableName=OraModel.simplificarNombreTabla(tableName);
						String[] pkCols=parseColSeries(pkColsList);
						OraTable t=model.getTableMandatory(tableName,line);
						for (String pkColName:pkCols)
							t.addPkCol(pkColName);
					}
					else if ((m=COMMENT_ER.matcher(line)).matches()) {
						String tableName=OraModel.simplificarNombreTabla(m.group(1));
						String colName=OraModel.simplificarNombreColumna(m.group(2));
						String comentario=m.group(3);
						OraTable.OraColumn col=model.getTableColumnMandatory(tableName,colName,line);
						col.setComentario(comentario);
					}
					else if ((m=FK1_ER.matcher(line)).matches()) {
						String srcTableName=OraModel.simplificarNombreTabla(m.group(1));
						String srcFkColsList=m.group(2);
						OraTable srcTable=model.getTableMandatory(srcTableName,line);
						line=in.readLine();
						if ((m=FK2_ER.matcher(line)).matches()) {
							String destTableName=OraModel.simplificarNombreTabla(m.group(1));
							String destFkColsList=m.group(2);
							String[] srcFkCols=parseColSeries(srcFkColsList);
							String[] destFkCols=parseColSeries(destFkColsList);
							srcTable.addFK(srcFkCols,destTableName,destFkCols);
						}
					}
					else if ((m=TABLECOMMENT_ER.matcher(line)).matches()) {
						String tableName=OraModel.simplificarNombreTabla(m.group(1));
						String comentario=m.group(2);
						OraTable table=model.getTableMandatory(tableName,line);
						table.setComment(comentario);
					}
				}
				else /*if (tablaActual!=null) */ {
					if ((m=FIN_TABLA_ER.matcher(line)).matches()) {
						tablaActual=null;
					}
					else if ((m=COLUMN_ER.matcher(line)).matches()) {
						String nombreColumna=m.group(1);
						String tipoColumna=m.group(2);
						String attrTipoColumna=m.groupCount()>2?m.group(3):null;
						logDebug("Parseada columna con nombreColumna=\""+nombreColumna+"\", tipoColumna=\""+tipoColumna+"\", attrTipoColumna=\""+attrTipoColumna+"\"");
						nombreColumna=OraModel.simplificarNombreColumna(nombreColumna);
						OraTable.OraColumn col=tablaActual.new OraColumn(nombreColumna,tipoColumna,attrTipoColumna);
						tablaActual.addNew(col);
					}
				}
			}
			return model;
		}
		catch (Exception e) {
			throw new MdlParseException("Error parseando entrada "+inputDescription,e);
		}
	}
	
	protected static String[] parseColSeries(String colSeries) {
		List<String> cols=new ArrayList<String>();
		boolean mas=true;
		int desde=0;
		while (mas) {
			int p=colSeries.indexOf(',', desde);
			String col;
			if (p<0) {
				col=colSeries.substring(desde, colSeries.length());
				mas=false;
			}
			else {
				col=colSeries.substring(desde,p);
				desde=p+1;
			}
			col=col.trim();
			col=OraModel.simplificarNombreColumna(col);
			cols.add(col);
		}
		return cols.toArray(new String[cols.size()]);
	}
	
	protected void logDebug(String msg) {
		//System.out.println("DEBUG: "+msg);
	}

	protected static void tempParse(String line) {
		Matcher m;
		System.out.println("Línea:\n"+line);
		if ((m=COLUMN_ER.matcher(line)).matches()) {
			String nombreColumna=m.group(1);
			String tipoColumna=m.group(2);
			String attrTipoColumna=m.groupCount()>2?m.group(3):null;
			System.out.println("COLUMN_ER: nombreColumna=\""+nombreColumna+"\", tipoColumna=\""+tipoColumna+"\", attrTipoColumna=\""+attrTipoColumna+"\"");
		}
		else
			System.out.println("COLUMN_ER: NO");
		if ((m=FIN_TABLA_ER.matcher(line)).matches()) {
			System.out.println("FIN_TABLA_ER: OK");
		}
		else
			System.out.println("FIN_TABLA_ER: NO");
		if ((m=FK1_ER.matcher(line)).matches()) {
			String srcTableName=OraModel.simplificarNombreTabla(m.group(1));
			String srcFkColsList=m.group(2);
			System.out.println("FK1_ER: srcTableName=\""+srcTableName+"\", srcFkColsList=\""+srcFkColsList+"\"");
		}
		else
			System.out.println("FK1_ER: NO");
		if ((m=FK2_ER.matcher(line)).matches()) {
			String destTableName=OraModel.simplificarNombreTabla(m.group(1));
			String destFkColsList=m.group(2);
			System.out.println("FK2_ER: destTableName=\""+destTableName+"\", destFkColsList=\""+destFkColsList+"\"");
		}
		else
			System.out.println("FK2_ER: NO");
	}
	public static void main(String[] args) throws Exception {
//		tempParse("   (\t\"COD_EXPEDIENTE\" VARCHAR2(30 CHAR), ");
//		tempParse("\t\"NUM_GARANTIA_PP\" NUMBER(3,0),");
//		tempParse("   ) SEGMENT CREATION IMMEDIATE ");
//		tempParse("\t\"FEC_CREACION\" TIMESTAMP (6), ");
//		tempParse("\t\"COD_DT\" VARCHAR2(5 CHAR) DEFAULT NULL, ");
//		tempParse("ALTER TABLE \"ECV_LD\".\"ECV_DAT_RIESGOS_EMI\" ADD CONSTRAINT \"FK_ECV_DAT_RIESGOS_EMI_FOR_COB\" FOREIGN KEY (\"COD_FORMA_COB\")");
//		tempParse("REFERENCES \"ECV_LD\".\"ECV_GEN_FORMA_COBRO\" (\"COD_FORMA_COB\") ENABLE;");

		String sqlDeveloperExportFile=args[0];
		//BufferedReader in=new BufferedReader(new FileReader(sqlDeveloperExportFile));
		BufferedReader in=new BufferedReader(new InputStreamReader(new FileInputStream(sqlDeveloperExportFile),"UTF-8"));
		OraModel model=(new ParseMdlFromSQLDeveloperExport()).parse(in,sqlDeveloperExportFile);
		System.out.println(model.toString());
	}
}
