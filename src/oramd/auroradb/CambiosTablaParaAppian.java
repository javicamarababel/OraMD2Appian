package oramd.auroradb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oramd.mdl.FK;
import oramd.mdl.OraModel;
import oramd.mdl.OraTable;

public class CambiosTablaParaAppian {
	protected OraTable table;
	protected List<OraTable.OraColumn> nuevasColsFk=new ArrayList<OraTable.OraColumn>();
	protected Map<String/*nombreCol*/,String/*comentario*/> columnasComentadas=new HashMap<String,String>();
	protected Map<String/*nombreCol*/,String/*sustituidaPor*/> columnasSustituidas=new HashMap<String,String>();
	protected List<FK> fks=new ArrayList<FK>();
	
	public CambiosTablaParaAppian(OraTable table) {
		this.table=table;
		cambiarFKs();
		quitarColumnas();
	}
	
	protected void cambiarFKs() {
		/* Para cada FK de Oracle:
		 * - Añadir columna idSeq de la tabla destino
		 * - Comentar las columnas origen
		 * - Añadir FK al idSeq de la tabla destino
		 */
		for (FK oraFk:table.getFKs()) {
			String nombreTablaDestino=Ora2AppianMySQL.normalizaNombre(oraFk.getDestTableName());
			String pkSeqDest=Ora2AppianMySQL.getSeqPkForTable(nombreTablaDestino);
			String nombreColumnaAppian=null;
			/* Si solo hay 1 columna, y no se llama igual que la PK de la tabla destino, entonces nombreColumnaAppian se basará en ese nombre
			 * En otro caso, nombreColumnaAppian=PK Appian de la tabla destino 
			 */
			if (oraFk.getSrcCols().length==1) {
				String nombreColSrc=oraFk.getSrcCols()[0];
				String nombreColDest=oraFk.getDestCols()[0];
				if (!nombreColSrc.equals(nombreColDest)) {
					String parteSignificativaNombreCol=getParteSignificativaNombreCol(nombreColSrc);
					nombreColumnaAppian=pkSeqDest+"_"+parteSignificativaNombreCol;
				}
			}
			if (nombreColumnaAppian==null)
				nombreColumnaAppian=pkSeqDest;
			
			// Añadir columna Appian
			OraTable.OraColumn colFk=table.new OraColumn(nombreColumnaAppian, null, null);
			// Si solo hay 1 columna origen, le ponemos el comentario que tenga esta; si tiene más, generamos uno
			String comentario=null;
			if (oraFk.getSrcCols().length==1) {
				OraTable.OraColumn oraCol=table.getColumn(oraFk.getSrcCols()[0]);
				comentario=oraCol.getComentario();
			}
			if (comentario==null)
				comentario="Referencia a tabla "+nombreTablaDestino;
			colFk.setComentario(comentario);
			colFk.setNullable(false); // Abajo puede cambiar
			nuevasColsFk.add(colFk);
	
			// Comentar las columnas origen
			for (String oraColNombre:oraFk.getSrcCols()) {
				columnasComentadas.put(oraColNombre,"Sustituida por columna "+nombreColumnaAppian+" que hace referencia a la tabla "+nombreTablaDestino);
				columnasSustituidas.put(oraColNombre, nombreColumnaAppian);
				// Si alguna columna es nulable, la nueva col también
				OraTable.OraColumn oraCol=table.getColumn(oraColNombre);
				if (oraCol.getNullable())
					colFk.setNullable(true);
			}
			
			// Añadir FK al idSeq de la tabla destino
			fks.add(new FK(new String[] {nombreColumnaAppian},nombreTablaDestino,new String[] {pkSeqDest}));
		}
	}

	protected void quitarColumnas() {
		for (OraTable.OraColumn col:table.getColumns()) {
			String oraColName=col.getName();
			if ("idn_idioma".equalsIgnoreCase(oraColName)) {
				columnasComentadas.put(oraColName,"Suprimida por innecesaria");
			}
		}
	}

	// Ej. COD_REQUISITO -> REQUISITO
	protected static String getParteSignificativaNombreCol(String nombreCol) {
		String parteSignificativaNombreCol;
		int p=nombreCol.indexOf("_");
		if (p>0)
			parteSignificativaNombreCol=nombreCol.substring(p+1);
		else
			parteSignificativaNombreCol=nombreCol;
		return parteSignificativaNombreCol;
	}
}

