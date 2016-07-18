package com.di.util.jdbc.template;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import com.di.util.jdbc.template.annotation.Column;
import com.di.util.jdbc.template.annotation.Id;
import com.di.util.jdbc.template.annotation.IgnoreUpdate;
import com.di.util.jdbc.template.annotation.JoinColumn;
import com.di.util.jdbc.template.annotation.ManyToOne;
import com.di.util.jdbc.template.annotation.OneToMany;
import com.di.util.jdbc.template.annotation.Table;

/**
 * @author di
 */
public class JdbcTemplate {
	private String driverClassName;
	private String username;
	private String password;
	private String url;
	private String fileName;
	private Connection con;
	private ResultSet res;
	private PreparedStatement pstmt;
	private Statement stmt;
	private long initTime;

	public JdbcTemplate() {
		fileName = "jdbc.properties";
		this.init();
	}

	public JdbcTemplate(String fileName) {
		this.fileName = fileName;
		this.init();
	}

	private void init() {
		Properties prop = new Properties();
		String path = "";
		try {
			path = JdbcTemplate.class.getClass().getResource("/").getPath() + fileName;
		} catch (NullPointerException nu) {
			try {
				path = Thread.currentThread().getContextClassLoader().getResource("").toURI().getPath();
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			path = path + fileName;
		}
		try {
			prop.load(new FileInputStream(new File(path)));
		} catch (FileNotFoundException e) {
			System.err.println(path + " not found");
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		driverClassName = prop.getProperty("driverClassName");
		url = prop.getProperty("url");
		username = prop.getProperty("username");
		password = prop.getProperty("password");
		if (driverClassName == null)
			System.err.println("driverClassName is null or not found");
		if (url == null)
			System.err.println("url is null or not found");
		if (username == null)
			System.err.println("username is null or not found");
		if (password == null)
			System.err.println("password is null or not found");
		try {
			Class.forName(driverClassName);
			DriverManager.setLoginTimeout(60);
			initTime = new Date().getTime();
			con = DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	private void reInit() {
		closeAll();
		try {
			con = DriverManager.getConnection(url, username, password);
			initTime = new Date().getTime();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void closeResultSetAndStatement() {
		try {
			if (res != null && !res.isClosed())
				res.close();
			if (pstmt != null && !pstmt.isClosed())
				pstmt.close();
			if (stmt != null && !stmt.isClosed())
				stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void closeAll() {
		try {
			if (res != null) {
				res.close();
				res = null;
			}
			if (pstmt != null) {
				pstmt.close();
				pstmt = null;
			}
			if (stmt != null) {
				stmt.close();
				stmt = null;
			}
			if (con != null) {
				con.close();
				con = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void closePreviousStatement() {
		assertTimeout();
		if (con != null) {
			closeResultSetAndStatement();
		} else {
			init();
		}
	}

	private void assertTimeout() {
		int preid = (int) ((new Date().getTime() - initTime) / 1000);
		if (preid >= 60) {
			this.reInit();
		}
	}

	public ResultSet createQuery(String sql) {
		ResultSet res1 = null;
		try {
			closePreviousStatement();
			stmt = con.createStatement();
			res1 = stmt.executeQuery(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeResultSetAndStatement();
		}
		return res1;
	}

	public ResultSet createPrepareQuery(String sql, Object... args) {
		ResultSet res1 = null;
		try {
			closePreviousStatement();
			pstmt = con.prepareStatement(sql);
			for (int i = 0; i < args.length; i++) {
				pstmt.setObject(i + 1, args[i]);
			}
			res1 = pstmt.executeQuery();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeResultSetAndStatement();
		}
		return res1;
	}

	public List<HashMap<String, String>> queryReturnMap(String sql) {
		List<HashMap<String, String>> list = new ArrayList<>();
		try {
			closePreviousStatement();
			stmt = con.createStatement();
			res = stmt.executeQuery(sql);
			while (res.next()) {
				HashMap<String, String> m = new HashMap<String, String>();
				ResultSetMetaData rsmd = res.getMetaData();
				int columnCount = rsmd.getColumnCount();
				for (int i = 1; i <= columnCount; i++) {
					String colName = rsmd.getColumnLabel(i);
					if (colName == null) {
						colName = rsmd.getColumnName(i);
					}
					m.put(colName, res.getString(i));
				}
				list.add(m);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeResultSetAndStatement();
		}
		return list;
	}

	public List<LinkedHashMap<String, Object>> queryReturnLinkedMap(String sql) {
		List<LinkedHashMap<String, Object>> list = new ArrayList<>();
		try {
			closePreviousStatement();
			stmt = con.createStatement();
			res = stmt.executeQuery(sql);
			while (res.next()) {
				LinkedHashMap<String, Object> m = new LinkedHashMap<String, Object>();
				ResultSetMetaData rsmd = res.getMetaData();
				int columnCount = rsmd.getColumnCount();
				for (int i = 1; i <= columnCount; i++) {
					String colName = rsmd.getColumnLabel(i);
					if (colName == null) {
						colName = rsmd.getColumnName(i);
					}
					m.put(colName, res.getObject(i));
				}
				list.add(m);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeResultSetAndStatement();
		}
		return list;
	}

	public List<Object> queryReturnObject(String sql, @SuppressWarnings("rawtypes") Class resultClass) {
		List<Object> list = new ArrayList<>();
		try {
			closePreviousStatement();
			stmt = con.createStatement();
			res = stmt.executeQuery(sql);
			while (res.next()) {
				Object obj = resultClass.newInstance();
				Field[] fs = obj.getClass().getDeclaredFields();
				for (Field f : fs) {
					f.setAccessible(true);
					String column = f.getName();
					if (f.isAnnotationPresent(Column.class)) {
						column = f.getAnnotation(Column.class).name();
					}
					try {
						f.set(obj, res.getObject(column));
					} catch (SQLException se) {
					}
				}
				list.add(obj);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeResultSetAndStatement();
		}
		return list;
	}

	public int executeUpdate(String sql) {
		int i = 0;
		try {
			closePreviousStatement();
			stmt = con.createStatement();
			i = stmt.executeUpdate(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeResultSetAndStatement();
		}
		return i;
	}

	public int executeUpdate(String sql, boolean b) {
		int i = 0;
		try {
			closePreviousStatement();
			con.setAutoCommit(b);
			stmt = con.createStatement();
			i = stmt.executeUpdate(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeResultSetAndStatement();
		}
		return i;
	}

	public boolean executeInsert(String sql) {
		boolean b = false;
		try {
			closePreviousStatement();
			stmt = con.createStatement();
			b = stmt.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeResultSetAndStatement();
		}
		return b;
	}

	public boolean executeObjectUpdate(Object o) {
		Field fs[] = o.getClass().getDeclaredFields();
		String tabName;
		if (o.getClass().isAnnotationPresent(Table.class)) {
			tabName = o.getClass().getAnnotation(Table.class).name();
		} else {
			tabName = o.getClass().getSimpleName();
		}
		StringBuilder sql = new StringBuilder("update ").append(tabName).append(" set ");
		String idName = "id";
		Object idValue = null;
		for (Field f : fs) {
			f.setAccessible(true);
			try {
				System.out.println(f.get(o));
				if (f.isAnnotationPresent(Id.class)) {
					idValue = f.get(o);
					if (f.isAnnotationPresent(Column.class)) {
						idName = f.getAnnotation(Column.class).name();
					} else {
						idName = f.getName();
					}
				} else if (f.isAnnotationPresent(Column.class)) {
					sql.append(f.getAnnotation(Column.class).name()).append("='").append(f.get(o)).append("',");
				} else if (f.isAnnotationPresent(JoinColumn.class) && f.isAnnotationPresent(ManyToOne.class)) {
				} else if (f.isAnnotationPresent(OneToMany.class)) {
				} else if (f.isAnnotationPresent(IgnoreUpdate.class)) {
				} else {
					sql.append(f.getName()).append("='").append(f.get(o)).append("',");
				}
			} catch (IllegalArgumentException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		String sq = sql.toString();
		sql = new StringBuilder(sq.substring(0, sq.lastIndexOf(",")));
		sql.append(" where ").append(idName).append("='").append(idValue).append("'");
		System.out.println(sql.toString());
		executeUpdate(sql.toString());
		closeResultSetAndStatement();
		return true;
	}

	// public static void main(String[] args) throws FileNotFoundException,
	// IOException {
	// JdbcTemplate jt = new JdbcTemplate("mysql.properties");
	// List<HashMap<String, String>> l = jt.createQueryReturnMapListed("select *
	// from goods where goods_id<100");
	// for(HashMap<String, String> m:l){
	// System.out.println(m.get("goods_id"));
	// }
	// }
}
