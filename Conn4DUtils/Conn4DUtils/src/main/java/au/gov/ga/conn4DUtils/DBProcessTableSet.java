package au.gov.ga.conn4DUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

public abstract class DBProcessTableSet extends DBOperation {

	protected Statement stmt;
	protected DateFormat df = new SimpleDateFormat("HH:mm:ss");

	public void batchProcess(String pattern) {
		try {
			List<String> list = this.listTables();
			List<String> filteredList = Lists.newArrayList(Collections2.filter(
					list, Predicates.containsPattern(pattern)));
			Collections.sort(filteredList,new Alphanum());
			filteredList.toString();
			for (String name : filteredList) {
					process(name);				
			}
		} catch (SQLException e) {
			e.printStackTrace();
			e.getNextException().printStackTrace();
		}
	}
	
	public void batchProcessViews(String pattern) {
		try {
			List<String> list = this.listViews();
			List<String> filteredList = Lists.newArrayList(Collections2.filter(
					list, Predicates.containsPattern(pattern)));
			Collections.sort(filteredList,new Alphanum());
			filteredList.toString();
			for (String name : filteredList) {
					process(name);				
			}
		} catch (SQLException e) {
			e.printStackTrace();
			e.getNextException().printStackTrace();
		}
	}
	
	public abstract void process (String table) throws SQLException ;
}