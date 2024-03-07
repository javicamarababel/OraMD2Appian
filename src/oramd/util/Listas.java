package oramd.util;

import java.util.Collection;

public class Listas {

	public static String listaToString(Object[] objs) {
		StringBuilder s=new StringBuilder();
		boolean first=true;
		for (Object obj:objs) {
			if (first)
				first=false;
			else {
				s.append(",");
			}
			s.append(obj.toString());
		}
		return s.toString();
	}
	
	public static String collToString(Collection col,String indent) {
		StringBuilder s=new StringBuilder();
		boolean first=true;
		for (Object obj:col) {
			if (first)
				first=false;
			else {
				s.append("\n");
			}
			s.append(indent);
			s.append(obj.toString());
		}
		return s.toString();
	}


}
