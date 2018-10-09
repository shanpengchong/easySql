package com.example.shyFly.easySql;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.example.shyFly.easySql.annotations.TableField;
import com.example.shyFly.easySql.annotations.TableId;
import com.example.shyFly.easySql.annotations.TableIgnore;
import com.example.shyFly.easySql.annotations.TableName;
import com.example.shyFly.easySql.bean.EasySqlMapResult;
import com.example.shyFly.easySql.bean.EasySqlResult;
import com.example.shyFly.easySql.bean.Page;
import com.example.shyFly.easySql.bean.SpliceBean;
import com.example.shyFly.easySql.config.EasySqlConfiguration;
import com.example.shyFly.easySql.config.LoggerUtil;
import com.example.shyFly.easySql.config.TransactionUtil;
import com.mysql.jdbc.Statement;

/**
 * easySql的具体执行类 内置基础增删改查和分页,事务</br>
 * 在使用之前请务必了解一下内容:</br>
 * 一:本工具提供了以下4个注解</br>
 * &nbsp;&nbsp;&nbsp;1:TableName(name="") 在你需要操作的bean类上添加，用来标注数据库表名,此为必填项</br>
 * &nbsp;&nbsp;&nbsp;2:TableId 在你需要操作的bean类属性上添加，用来标注数据库表主键名称，此为必填项</br>
 * &nbsp;&nbsp;&nbsp;3:TableField(name="")
 * 在你需要操作的bean类属性上添加，用来标注数据库表字段,如一致，则非必填</br>
 * &nbsp;&nbsp;&nbsp;4:TableIgnore
 * 在你需要操作的bean类属性上添加，用来标注数据库表中没有的字段，新增时不会添加进表</br>
 * 二:如果你需要操作的类属性有非基本数据，则无法使用本工具
 * 
 * @author shyFly
 *
 */
public class EasySqlExecution {
	private DataSource dataSource;
	private static Object lock = new Object();

	/** 默认连接池别名*/
	private static final String CONFIG_DEFAULT = "CONFIG_DEFAULT";
	private static final String USER_DEFAULT = "USER_DEFAULT";
	/** 默认数据源及连接池，从项目中读取*/
	public EasySqlExecution() {
		if(EasySqlConfiguration.dataSource == null){
			EasySqlConfiguration.dataSource = setDataSource(CONFIG_DEFAULT,null, null, null, null);
		}
		this.dataSource = EasySqlConfiguration.dataSource;
	}

	/**
	 * 根据标记获取连接池,如果不存在则创建
	 * @param flag 标记
	 */
	public EasySqlExecution(String flag){
		this.dataSource = setDataSource(flag,null, null, null, null);
	}
	
	/**
	 * 传入一个连接池，并作为本次对象使用的连接池
	 * @param dataSource 外部传入连接池
	 */
	public EasySqlExecution(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	/**
	 * 传入一个连接池作为本次对象使用的连接池。
	 * 配置此连接池标记,并将此连接池交予多数据源方案管理
	 * @param flag 标记
	 * @param dataSource 外部传入连接池
	 */
	public EasySqlExecution(String flag,DataSource dataSource) {
		this.dataSource = setDataSource(flag,dataSource, null, null, null);
	}
	
	/**
	 * 传入一个数据源,easySql将为其配置连接池并交予多数据源方案管理
	 * @param url 数据源连接的url
	 * @param username 数据源连接的username
	 * @param password 数据源连接的password
	 */
	public EasySqlExecution(String url, String username, String password) {
		this.dataSource = setDataSource(USER_DEFAULT, null, url, username, password);
	}
	
	/**
	 * 传入一个数据源,easySql将为其配置连接池并交予多数据源方案管理
	 * @param flag 标记
	 * @param url 数据源连接的url
	 * @param username 数据源连接的username
	 * @param password 数据源连接的password
	 */
	public EasySqlExecution(String flag,String url, String username, String password) {
		this.dataSource = setDataSource(flag,null, url, username, password);
	}

	/**
	 * 获取当前连接池
	 * @param flag
	 * @return
	 */
	public DataSource getDataSource(){
		return this.dataSource;
	}
	/** 事务处理工具*/
	private TransactionUtil transactionUtil = new TransactionUtil();

	/**
	 * 多数据源存储
	 * @param flag
	 * @param dataSource
	 * @param url
	 * @param username
	 * @param password
	 * @return
	 */
	private DataSource setDataSource(String flag,DataSource dataSource, String url, String username, String password) {
		if (flag == null) {
			throw new RuntimeException("连接池别名不能为空,或请使用默认构造器");
		}
		if (EasySqlConfiguration.getDataSourceMapValue(flag) == null) {
			synchronized (lock) {
				if (EasySqlConfiguration.getDataSourceMapValue(flag) == null) {
					if (dataSource != null) {
						EasySqlConfiguration.setDataSourceMapValue(flag, dataSource);
					} else {
						EasySqlConfiguration.url = url;
						EasySqlConfiguration.username = username;
						EasySqlConfiguration.password = password;
						EasySqlConfiguration.setDataSourceMapValue(flag, null);
					}
				}
			}
		}
		return EasySqlConfiguration.getDataSourceMapValue(flag);
	}

	/** 获取当前持有的connection连接*/
	public Connection getConcurrentConnect(){
		Connection connection = null;
		if (TransactionUtil.localConnection.get() != null) {
			return TransactionUtil.localConnection.get();
		}
		try {
			connection = this.dataSource.getConnection();
			TransactionUtil.localConnection.set(connection);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return connection;
	}
	/** 获取当前连接,并默认手动提交事务 */
	private Connection getConnect() {
		Connection connection = getConcurrentConnect();
		try {
			connection.setAutoCommit(false);
			TransactionUtil.isTransaction.set(false);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return connection;
	}
	
	/** 新增时获取主键 */
	public String exeuAdd(String sql,Object...params){
		String id =  exeuAdd(sql,Arrays.asList(params));
		hasTransaction();
		toCommit();
		return id;
	}

	/** 新增时获取主键 */
	private String exeuAdd(String sql,List<Object> params) {
		if(!sql.toUpperCase().startsWith("INSERT")){
			throw new RuntimeException("当前非新增语句");
		}
		Connection connect = getConnect();
		PreparedStatement statement;
		try {
			statement = connect.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			for (int i = 0; i < params.size(); i++) {
				statement.setObject(i + 1, params.get(i));
			}
			exeuUpdate(statement);
			LoggerUtil.info(this.getClass(), "==============================");
			LoggerUtil.info(this.getClass(), "执行新增语句:" + sql.toString());
			LoggerUtil.info(this.getClass(), "执行参数为:" + params);
			LoggerUtil.info(this.getClass(), "==============================");
			ResultSet result = statement.getGeneratedKeys();
			if (result.next()) {
				return result.getObject(1) + "";
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	/** 装载statment对象 */
	private PreparedStatement exeuSql(String sql, List<Object> params) {
		Connection connect = getConnect();
		PreparedStatement statement;
		try {
			statement = connect.prepareStatement(sql);
			LoggerUtil.info(this.getClass(), "==============================");
			LoggerUtil.info(this.getClass(), "执行语句:" + sql);
			LoggerUtil.info(this.getClass(), "执行参数为:" + params);
			LoggerUtil.info(this.getClass(), "==============================");
			if (params == null || params.size() == 0) {
				return statement;
			}
			for (int i = 0; i < params.size(); i++) {
				statement.setObject(i + 1, params.get(i));
			}
			return statement;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/** 多条查询 */
	private List<Map<String, Object>> exeuQuery(PreparedStatement statement) {
		ResultSetMetaData res = null;
		try {
			ResultSet result = statement.executeQuery();
			res = result.getMetaData();
			int count = res.getColumnCount();
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			while (result.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 1; i <= count; i++) {
					map.put(res.getColumnLabel(i), result.getObject(i));
				}
				list.add(map);
			}
			return list;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/** 修改 */
	private int exeuUpdate(PreparedStatement statement) {
		try {
			int count = statement.executeUpdate();
			return count;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/** 查询条目数 */
	private int exeuCount(PreparedStatement statement) {
		try {
			ResultSet result = statement.executeQuery();
			if (result.first()) {
				return result.getInt(1);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return 0;
	}

	/**
	 * 设置提交模式
	 * @param autoCommit true:自动提交
	 */
	public void setAutoCommit(boolean autoCommit){
		TransactionUtil.autoCommit.set(autoCommit);
	}
	/**
	 * 当前方法是否含有事务
	 * 
	 */
	private void hasTransaction() {
		if (transactionUtil.isTranMothed()) {
			TransactionUtil.isTransaction.set(true);
			LoggerUtil.info(this.getClass(), "====当前有事务开启===");
		}
	}

	/** 提交事务 */
	private void toCommit() {
		try {
			if (!TransactionUtil.isTransaction.get()) {
				if (TransactionUtil.localConnection.get() != null) {
					TransactionUtil.localConnection.get().commit();
					TransactionUtil.localConnection.get().close();
					TransactionUtil.localConnection.remove();
				}
				if (TransactionUtil.isTransaction != null) {
					TransactionUtil.isTransaction.remove();
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/** 提交事务 */
	public void commit() {
		TransactionUtil.isTransaction.set(false);
		toCommit();
		LoggerUtil.info(this.getClass(), "====当前事务被提交===");
	}

	/** 回滚事务 */
	public void rollBack() {
		try {
			if (TransactionUtil.localConnection.get() != null) {
				TransactionUtil.localConnection.get().rollback();
				TransactionUtil.localConnection.get().close();
				TransactionUtil.localConnection.remove();
				if (TransactionUtil.isTransaction != null) {
					TransactionUtil.isTransaction.remove();
				}
				LoggerUtil.info(this.getClass(), "====当前事务已回滚===");
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 自定义语句分页查询
	 * @param sql 自定义sql
	 * @param currentPageParam 当前页
	 * @param showCountParam 每页显示条目数
	 * @param params sql参数
	 * @return
	 */
	public EasySqlMapResult queryListPageCustomizeSql(String sql,Integer currentPageParam, Integer showCountParam){
		Object[] objs = new Object[] {};
		return queryListPageCustomizeSql(sql, currentPageParam, showCountParam, objs);
	}
	
	/**
	 * 自定义语句分页查询
	 * @param sql 自定义sql
	 * @param currentPageParam 当前页
	 * @param showCountParam 每页显示条目数
	 * @param params sql参数
	 * @return
	 */
	public EasySqlMapResult queryListPageCustomizeSql(String sql,Integer currentPageParam, Integer showCountParam,Object... params){
		if (sql != null && !sql.trim().toUpperCase().startsWith("SELECT")) {
			throw new RuntimeException("当前非查询语句");
		}
		String countSql = "select count(1) "+sql.substring(sql.indexOf("from"));
		int totalResult = exeuCount(exeuSql(countSql, Arrays.asList(params)));
		EasySqlMapResult easySqlResult = new EasySqlMapResult();
		if (totalResult == 0) {
			return easySqlResult;
		}
		Page page = new Page();
		page.setShowCount(showCountParam);
		page.setCurrentPage(currentPageParam);
		// 设置总记录数
		page.setTotalResult(totalResult);
		// 设置总页数
		page.setTotalPage((totalResult - 1) / page.getShowCount() + 1);
		Integer currentPage = page.getCurrentPage();
		Integer showCount = page.getShowCount();
		// 开始的数据
		Integer start = (currentPage - 1) * showCount;
		// 结束的数据
		Integer end = showCount;
		// 如果当前页大于总页数,则返回最后一页
		if (start >= totalResult) {
			start = (page.getTotalPage() - 1) * showCount;
			end = -1;
		}
		start = start < 0 ? 0 : start;
		String limitSql = sql + " limit " + start + "," + end;
		List<Map<String, Object>> list = queryCustomizeSql(limitSql, params);
		if (list.size() > 0) {
			easySqlResult.setResultList(list);
			easySqlResult.setPage(page);
		}
		return easySqlResult;
	}
	
	/**
	 * 自定义语句分页查询
	 * @param sql 自定义sql
	 * @param clazz 封装的返回值类型
	 * @param currentPageParam 当前页
	 * @param showCountParam 每页显示条目数
	 * @return
	 */
	public <T> EasySqlResult<T> queryListPageCustomizeSql(String sql,Class<T> clazz,Integer currentPageParam, Integer showCountParam){
		Object[] objs = new Object[] {};
		return queryListPageCustomizeSql(sql, clazz, currentPageParam, showCountParam, objs);
	}

	/**
	 * 自定义语句分页查询
	 * @param sql 自定义sql
	 * @param clazz 封装的返回值类型
	 * @param currentPageParam 当前页
	 * @param showCountParam 每页显示条目数
	 * @param params sql参数
	 * @return
	 */
	public <T> EasySqlResult<T> queryListPageCustomizeSql(String sql,Class<T> clazz,Integer currentPageParam, Integer showCountParam,Object... params){
		if (sql != null && !sql.trim().toUpperCase().startsWith("SELECT")) {
			throw new RuntimeException("当前非查询语句");
		}
		String countSql = "select count(1) "+sql.substring(sql.indexOf("from"));
		int totalResult = exeuCount(exeuSql(countSql, Arrays.asList(params)));
		EasySqlResult<T> easySqlResult = new EasySqlResult<T>();
		if (totalResult == 0) {
			return easySqlResult;
		}
		Page page = new Page();
		page.setShowCount(showCountParam);
		page.setCurrentPage(currentPageParam);
		// 设置总记录数
		page.setTotalResult(totalResult);
		// 设置总页数
		page.setTotalPage((totalResult - 1) / page.getShowCount() + 1);
		Integer currentPage = page.getCurrentPage();
		Integer showCount = page.getShowCount();
		// 开始的数据
		Integer start = (currentPage - 1) * showCount;
		// 结束的数据
		Integer end = showCount;
		// 如果当前页大于总页数,则返回最后一页
		if (start >= totalResult) {
			start = (page.getTotalPage() - 1) * showCount;
			end = -1;
		}
		start = start < 0 ? 0 : start;
		String limitSql = sql + " limit " + start + "," + end;
		List<T> list = new ArrayList<T>();
		list = queryCustomizeSql(limitSql, clazz, params);
		if (list.size() > 0) {
			easySqlResult.setResultList(list);
			easySqlResult.setPage(page);
		}
		return easySqlResult;
	}
	/**
	 * 自定义语句查询
	 * 
	 * @param sql 自定义sql
	 * @param params 自定义sql的参数
	 * @return
	 */
	public Map<String, Object> queryOneCustomizeSql(String sql){
		Object[] objs = new Object[] {};
		return queryOneCustomizeSql(sql,objs);
	}
	/**
	 * 自定义语句查询
	 * 
	 * @param sql 自定义sql
	 * @param params 自定义sql的参数
	 * @return
	 */
	public Map<String, Object> queryOneCustomizeSql(String sql, Object... params){
		if(sql.toUpperCase().contains("LIMIT")){
			sql = sql.substring(0,sql.indexOf("limit"))+" limit 1";
		}else{
			sql = sql +" limit 1";
		}
		List<Map<String, Object>> list = queryCustomizeSql(sql,params);
		if(list!=null && list.size()==0){
			return null;
		}
		return list.get(0);
	}
	/**
	 * 自定义语句查询
	 * 
	 * @param sql
	 * @return
	 */
	public <T>T queryOneCustomizeSql(String sql,Class<T> clazz) {
		Object[] objs = new Object[] {};
		return queryOneCustomizeSql(sql, clazz, objs);
	}
	
	/**
	 * 自定义语句查询
	 * @param sql 自定义sql
	 * @param clazz 返回值类型
	 * @param params 自定义sql的参数
	 * @return
	 */
	public <T>T queryOneCustomizeSql(String sql,Class<T> clazz, Object... params){
		if(sql.toUpperCase().contains("LIMIT")){
			sql = sql.substring(0,sql.indexOf("limit"))+" limit 1";
		}else{
			sql = sql +" limit 1";
		}
		List<T> list = queryCustomizeSql(sql, clazz, params);
		if(list==null || list.size()==0){
			return null;
		}
		return list.get(0);
	}
	
	/**
	 * 自定义语句查询
	 * 
	 * @param sql 自定义sql
	 * @param params 自定义sql的参数
	 * @return
	 */
	public List<Map<String, Object>> queryCustomizeSql(String sql, Object... params) {
		if (sql != null && !sql.trim().toUpperCase().startsWith("SELECT")) {
			throw new RuntimeException("当前非查询语句");
		}
		List<Object> list = Arrays.asList(params);
		List<Map<String, Object>> result = exeuQuery(exeuSql(sql, list));
		hasTransaction();
		toCommit();
		if(result == null){
			result = new ArrayList<Map<String,Object>>();
		}
		return result;
	}

	/**
	 * 自定义语句查询
	 * 
	 * @param sql
	 * @return
	 */
	public List<Map<String, Object>> queryCustomizeSql(String sql) {
		Object[] objs = new Object[] {};
		return queryCustomizeSql(sql, objs);
	}
	
	/**
	 * 自定义语句查询
	 * 
	 * @param sql
	 * @return
	 */
	public <T>List<T> queryCustomizeSql(String sql,Class<T> clazz) {
		Object[] objs = new Object[] {};
		return queryCustomizeSql(sql, clazz, objs);
	}
	
	
	
	/**
	 * 自定义语句查询
	 * @param sql 自定义sql
	 * @param clazz 返回值类型
	 * @param params 自定义sql的参数
	 * @return
	 */
	public <T>List<T> queryCustomizeSql(String sql,Class<T> clazz, Object... params) {
		if (sql != null && !sql.trim().toUpperCase().startsWith("SELECT")) {
			throw new RuntimeException("当前非查询语句");
		}
		List<Object> list = Arrays.asList(params);
		List<Map<String, Object>> result = exeuQuery(exeuSql(sql, list));
		hasTransaction();
		toCommit();
		if(result == null){
			result = new ArrayList<Map<String,Object>>();
		}
		return mapToBean(result, clazz);
	}
	/**
	 * 自定义语句修改
	 * 
	 * @param sql 自定义sql
	 * @param params 自定义sql的参数
	 * @return
	 */
	public int updateCustomizeSql(String sql, Object... params) {
		if (sql != null && sql.trim().toUpperCase().startsWith("SELECT")) {
			throw new RuntimeException("当前非修改语句");
		}
		List<Object> list = Arrays.asList(params);
		int count = exeuUpdate(exeuSql(sql, list));
		hasTransaction();
		toCommit();
		return count;
	}

	/**
	 * 自定义语句修改
	 * 
	 * @param sql
	 * @return
	 */
	public int updateCustomizeSql(String sql) {
		Object[] objs = new Object[] {};
		return updateCustomizeSql(sql, objs);
	}

	/**
	 * 依据不为空的条件查询多条数据
	 * 
	 * @return
	 */
	public <T> List<T> select(T obj) {
		return select(obj,-1, null, null, null, false);
	}

	/**
	 * 依据不为空的字段查询,支持排序
	 * 
	 * @param obj
	 * @param orderByTableField
	 * @param desc
	 * @return
	 */
	public <T> List<T> select(T obj, String orderByTableField, boolean desc) {
		return select(obj,-1, null, null, orderByTableField, desc);
	}

	/**
	 * 依据不为空的属性查询，支持聚合
	 * 
	 * @param obj
	 * @param beans
	 * @return
	 */
	public <T> List<T> select(T obj, SpliceBean... beans) {
		return select(obj,-1, null, null, null, false, beans);
	}

	/**
	 * 依据不为空属性查询，支持聚合与排序
	 * 
	 * @param obj
	 * @param orderByTableField
	 * @param desc
	 * @param beans
	 * @return
	 */
	public <T> List<T> select(T obj, String orderByTableField, boolean desc, SpliceBean... beans) {
		return select(obj,-1, null, null, orderByTableField, desc, beans);

	}
	
	/**
	 * 依据不为空的条件查询多条数据
	 * 
	 * @return
	 */
	public <T> T selectOne(T obj) {
		return selectOne(obj, null, false);
	}

	/**
	 * 依据不为空的字段查询,支持排序
	 * 
	 * @param obj
	 * @param orderByTableField
	 * @param desc
	 * @return
	 */
	public <T> T selectOne(T obj, String orderByTableField, boolean desc) {
		SpliceBean[] beans = new SpliceBean[]{};
		return selectOne(obj, orderByTableField, desc,beans);
	}

	/**
	 * 依据不为空的属性查询，支持聚合
	 * 
	 * @param obj
	 * @param beans
	 * @return
	 */
	public <T> T selectOne(T obj, SpliceBean... beans) {
		return selectOne(obj, null,  false, beans);
	}

	/**
	 * 依据不为空属性查询，支持聚合与排序
	 * 
	 * @param obj
	 * @param orderByTableField
	 * @param desc
	 * @param beans
	 * @return
	 */
	public <T> T selectOne(T obj, String orderByTableField, boolean desc, SpliceBean... beans) {
		List<T> list = select(obj,1, null, null, orderByTableField, desc, beans);
		if(list!=null && list.size()==0){
			return null;
		}
		return list.get(0);
	}

	/**
	 * 依据不为空的条件查询多条数据
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private <T> List<T> select(T obj,int limit, StringBuffer sql, List<Object> params, String orderByTableField, boolean desc,
			SpliceBean... beans) {
		String tableName = getTableName(obj);
		if (tableName == null) {
			throw new RuntimeException("数据库表名为空，或无@TableName注解");
		}
		Map<String, Object> attributes = getAttributes(obj);
		if (attributes == null) {
			throw new RuntimeException("参数对象属性解析出现异常");
		}
		if (sql == null) {
			Map<String, Object> map = creatSelectSql(obj, sql, attributes, params);
			sql = (StringBuffer) map.get("sql");
			params = (List<Object>) map.get("params");
		}
		if (beans.length > 0) {
			for (int i = 0; i < beans.length; i++) {
				sql.append(beans[i].getSpliceSql());
				params.addAll(beans[i].getSpliceParam());
			}
		}
		if (orderByTableField != null) {
			String isdesc = desc ? "desc" : "";
			sql.append(" order by " + orderByTableField + " " + isdesc);
		}
		if(limit > 0){
			sql.append(" limit "+limit);
		}
		List<Map<String, Object>> list = exeuQuery(exeuSql(sql.toString(), params));
		toCommit();
		if (list == null) {
			return new ArrayList<T>();
		}
		List<T> resulTs = mapToBean(list, obj);
		return resulTs;
	}

	/**
	 * 查询总条数
	 * 
	 * @param obj
	 * @return
	 */
	public <T> Integer selectCount(T obj) {
		SpliceBean[] objs = new SpliceBean[] {};
		return selectCount(obj, objs);
	}
	
	/**
	 * 查询总条数
	 * 
	 * @param obj
	 * @return
	 */
	public <T> Integer selectCount(T obj, SpliceBean... beans) {
		Map<String, Object> map = creatSelectSql(obj, null, null, null);
		StringBuffer sql = (StringBuffer) map.get("sql");
		@SuppressWarnings("unchecked")
		List<Object> params = (List<Object>) map.get("params");
		if (beans.length > 0) {
			for (int i = 0; i < beans.length; i++) {
				sql.append(beans[i].getSpliceSql());
				params.addAll(beans[i].getSpliceParam());
			}
		}
		int count = exeuCount(exeuSql(sql.toString(), params));
		toCommit();
		LoggerUtil.info(this.getClass(), "==============================");
		LoggerUtil.info(this.getClass(), "执行查询语句:" + sql.toString().replace("*", "count(1)"));
		LoggerUtil.info(this.getClass(), "执行参数为:" + params);
		LoggerUtil.info(this.getClass(), "==============================");
		return count;
	}

	/** 拼装基础查询语句 */
	private <T> Map<String, Object> creatSelectSql(T obj, StringBuffer sql, Map<String, Object> attributes,
			List<Object> params) {
		if (sql == null) {
			sql = new StringBuffer();
			String tableName = getTableName(obj);
			sql.append("select * from " + tableName + " where 1=1 ");
			attributes = getAttributes(obj);
			for (String key : attributes.keySet()) {
				sql.append("and " + key + "=? ");
			}
			params = new ArrayList<Object>();
			for (String key : attributes.keySet()) {
				params.add(attributes.get(key));
			}
		}
		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("params", params);
		resultMap.put("sql", sql);
		return resultMap;
	}

	/**
	 * 分页查询（依据不为空的条件查询） 查询完毕将page信息回填到参数bean的page属性中
	 * 
	 * @param obj
	 * @return
	 */
	public <T> EasySqlResult<T> listPage(T obj, Integer currentPageParam, Integer showCountParam,
			String orderByTableField, boolean desc, SpliceBean... beans) {
		if (showCountParam <= 0) {
			throw new RuntimeException("每页显示条数不能小于0");
		}
		String tableName = getTableName(obj);
		if (tableName == null) {
			throw new RuntimeException("数据库表名为空，或无@TableName注解");
		}
		EasySqlResult<T> easySqlResult = new EasySqlResult<T>();
		if (currentPageParam == null || showCountParam == null) {
			easySqlResult.setResultList(select(obj));
			return easySqlResult;
		}
		Map<String, Object> result = creatSelectSql(obj, null, null, null);
		// 原sql
		StringBuffer old = (StringBuffer) result.get("sql");
		@SuppressWarnings("unchecked")
		List<Object> params = (List<Object>) result.get("params");
		String replace = old.toString().replace("*", "count(1) totalResult");
		StringBuffer sql = new StringBuffer(replace);
		if (beans.length > 0) {
			for (int i = 0; i < beans.length; i++) {
				sql.append(beans[i].getSpliceSql());
				params.addAll(beans[i].getSpliceParam());
			}
		}
		// 获取总记录数
		int totalResult = exeuCount(exeuSql(sql.toString(), params));
		if (totalResult == 0) {
			return easySqlResult;
		}
		Page page = new Page();
		page.setShowCount(showCountParam);
		page.setCurrentPage(currentPageParam);
		// 设置总记录数
		page.setTotalResult(totalResult);
		// 设置总页数
		page.setTotalPage((totalResult - 1) / page.getShowCount() + 1);
		Integer currentPage = page.getCurrentPage();
		Integer showCount = page.getShowCount();
		// 开始的数据
		Integer start = (currentPage - 1) * showCount;
		// 结束的数据
		Integer end = showCount;
		// 如果当前页大于总页数,则返回最后一页
		if (start >= totalResult) {
			start = (page.getTotalPage() - 1) * showCount;
			end = -1;
		}
		start = start < 0 ? 0 : start;
		// 再次拼装sql
		String limit = sql.toString().replace("count(1) totalResult", "*");
		if (orderByTableField != null) {
			String isdesc = desc == false ? "" : "desc";
			limit = limit + " order by " + orderByTableField + " " + isdesc;
		}
		limit = limit + " limit " + start + "," + end;
		List<T> list = select(obj,-1, new StringBuffer(limit), params, orderByTableField, desc, beans);
		if (list.size() > 0) {
			easySqlResult.setResultList(list);
			easySqlResult.setPage(page);
		}
		return easySqlResult;
	}

	/**
	 * 分页，不排序
	 * 
	 * @param obj
	 * @param currentPageParam
	 * @param showCountParam
	 * @return
	 */
	public <T> EasySqlResult<T> listPage(T obj, Integer currentPageParam, Integer showCountParam) {
		return listPage(obj, currentPageParam, showCountParam, null, false);
	}

	/**
	 * 分页查询，支持排序
	 * 
	 * @param obj
	 * @param currentPageParam
	 * @param showCountParam
	 * @param orderByTableField
	 * @param desc
	 * @return
	 */
	public <T> EasySqlResult<T> listPage(T obj, Integer currentPageParam, Integer showCountParam,
			String orderByTableField, boolean desc) {
		SpliceBean[] beans = new SpliceBean[] {};
		return listPage(obj, currentPageParam, showCountParam, orderByTableField, desc, beans);
	}

	/**
	 * 分页查询，支持聚合
	 * 
	 * @param obj
	 * @param currentPageParam
	 * @param showCountParam
	 * @param beans
	 * @return
	 */
	public <T> EasySqlResult<T> listPage(T obj, Integer currentPageParam, Integer showCountParam, SpliceBean... beans) {
		return listPage(obj, currentPageParam, showCountParam, null, false, beans);
	}

	/**
	 * 添加数据(自动补填id)
	 * 
	 * @param obj
	 * @return
	 */
	public <T> T add(T obj) {
		// 自增主键不允许加入
		if (getTableIdValue(obj) != null) {
			throw new RuntimeException("主键:" + getTableIdName(obj) + "不允许作为参数");
		}
		String tableName = getTableName(obj);
		if (tableName == null) {
			throw new RuntimeException("数据库表名为空，或无@TableName注解");
		}
		Map<String, Object> attributes = getAttributes(obj);
		// 不能添加空数据
		if (attributes == null || attributes.isEmpty()) {
			throw new RuntimeException("数据库表不允许添加空数据");
		}
		StringBuffer sql = new StringBuffer();
		List<Object> params = new ArrayList<Object>();
		sql.append("insert into " + tableName + " (");
		for (String key : attributes.keySet()) {
			sql.append(key + " ,");
			params.add(attributes.get(key));
		}
		sql.delete(sql.length() - 1, sql.length());
		sql.append(" )values( ");
		for (@SuppressWarnings("unused") String key : attributes.keySet()) {
			sql.append(" ?,");
//			if (attributes.get(key) instanceof String) {
//				sql.append("'" + attributes.get(key) + "' , ");
//			} else {
//				sql.append(" " + attributes.get(key) + " , ");
//			}
		}
		sql.delete(sql.length() - 1, sql.length());
		sql.append(" )");
		String id = exeuAdd(sql.toString(),params);
		hasTransaction();
		toCommit();
		String tableIdName = getTableIdJavaName(obj);
		try {
			if (tableIdName == null) {
				throw new RuntimeException("数据插入成功,但未找到@TableId 注解,无法将主键返回");
			}
			Field idField = obj.getClass().getDeclaredField(tableIdName);
			if (idField.getGenericType().toString().equals("class java.lang.Integer")) {
				idField.setAccessible(true);
				idField.set(obj, Integer.valueOf(id));
				idField.setAccessible(false);
			} else if (idField.getGenericType().toString().equals("class java.lang.Long")) {
				idField.setAccessible(true);
				idField.set(obj, Long.valueOf(id));
				idField.setAccessible(false);
			} else {
				throw new RuntimeException("数据插入成功,但主键类型不匹配");
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return obj;
	}

	/**
	 * 修改数据（依据id修改）
	 * 
	 * @param obj
	 * @return
	 */
	public <T> Integer update(T obj, SpliceBean... beans) {
		String tableName = getTableName(obj);
		if (tableName == null) {
			throw new RuntimeException("数据库表名为空，或无@TableName注解");
		}
		// 检查参数是否为空
		boolean beansCheck = false;
		StringBuffer check = new StringBuffer();
		if (beans == null || beans.length == 0) {
			beansCheck = true;
		} else {
			for (SpliceBean bean : beans) {
				check.append(bean.getSpliceSql());
			}
			if (check.toString().equals("")) {
				beansCheck = true;
			}
		}
		// id不能为空
		if (getTableIdValue(obj) == null && beansCheck) {
			throw new RuntimeException("主键值为空，或无@TableId注解");
		}
		Map<String, Object> attributes = getAttributes(obj);
		//参数中的主键
		Object param_id = attributes.get(getTableIdName(obj));
		attributes.remove(param_id);
		if (attributes == null || attributes.isEmpty()) {
			throw new RuntimeException("参数属性为空");
		}
		StringBuffer sql = new StringBuffer();
		sql.append("update " + tableName + " set ");
		for (String key : attributes.keySet()) {
			sql.append(key + " = ? ,");
		}
		sql.delete(sql.length() - 1, sql.length());
		List<Object> params = new ArrayList<Object>();
		for (String key : attributes.keySet()) {
			params.add(attributes.get(key));
		}
		if (!beansCheck) {
			sql.append(" where 1=1 ").append(check.toString());
			for (SpliceBean object : beans) {
				params.addAll(object.getSpliceParam());
			}
			if(param_id != null){
				sql.append(" and "+ getTableIdName(obj) + " = ?");
				params.add(getTableIdValue(obj));
			}
		} else {
			sql.append(" where " + getTableIdName(obj) + " = ?");
			params.add(getTableIdValue(obj));
		}
		int num = exeuUpdate(exeuSql(sql.toString(), params));
		hasTransaction();
		toCommit();
		return num;
	}

	public <T> Integer update(T obj) {
		SpliceBean[] beans = new SpliceBean[] {};
		return update(obj, beans);
	}

	/**
	 * 删除数据(依据不为空的属性删除,支持聚合)
	 * 
	 * @param obj
	 * @return 影响行数
	 */
	public <T> Integer delete(T obj, SpliceBean... beans) {
		String tableName = getTableName(obj);
		if (tableName == null) {
			return null;
		}
		Map<String, Object> attributes = getAttributes(obj);
		// 属性为空不允许删除全表
		boolean beansCheck = false;
		if (beans == null || beans.length == 0) {
			beansCheck = true;
		} else {
			StringBuffer check = new StringBuffer();
			for (SpliceBean bean : beans) {
				check.append(bean.getSpliceSql());
			}
			if (check.toString().equals("")) {
				beansCheck = true;
			}
		}
		if (beansCheck && (attributes == null || attributes.isEmpty())) {
			throw new RuntimeException("属性为空,暂不允许删除全表数据");
		}
		StringBuffer sql = new StringBuffer();
		sql.append("delete from " + tableName + " where 1=1 ");
		for (String key : attributes.keySet()) {
			sql.append("and " + key + " =? ");
		}
		List<Object> params = new ArrayList<Object>();
		for (String key : attributes.keySet()) {
			params.add(attributes.get(key));
		}
		if (beans.length > 0) {
			for (int i = 0; i < beans.length; i++) {
				sql.append(beans[i].getSpliceSql());
				params.addAll(beans[i].getSpliceParam());
			}
		}
		int num = exeuUpdate(exeuSql(sql.toString(), params));
		hasTransaction();
		toCommit();
		return num;
	}

	/**
	 * 依据不为空的属性删除数据
	 * 
	 * @param obj
	 * @return
	 */
	public <T> Integer delete(T obj) {
		SpliceBean[] bus = new SpliceBean[] {};
		return delete(obj, bus);
	}

	/**
	 * 获取表名
	 * 
	 * @param <T>
	 */
	private <T> String getTableName(T t) {
		Class<?> tClass = t.getClass();
		String tableName = "";
		// 获取表名注解
		TableName annotation = tClass.getDeclaredAnnotation(TableName.class);
		if (annotation != null) {
			if(!annotation.name().equals("")){
				tableName = annotation.name();
			}
			if(!annotation.value().equals("")){
				tableName = annotation.value();
			}
			if(!tableName.equals("")){
				return tableName;
			}
		}
		return null;
	}

	/**
	 * 获取主键id的名称
	 * 
	 * @param <T>
	 */
	@SuppressWarnings("rawtypes")
	private <T> String getTableIdName(Object t) {
		String tableId = null;
		for (Class clazz = t.getClass(); !clazz.equals(Object.class); clazz = clazz.getSuperclass()) {
			Field[] fs = clazz.getDeclaredFields();
			for (Field f : fs) {
				// 获取tableId注解
				TableId annotation = f.getDeclaredAnnotation(TableId.class);
				TableField tableField = f.getDeclaredAnnotation(TableField.class);
				if (annotation != null) {
					if (tableField != null) {
						if(!tableField.name().equals("")){
							tableId = tableField.name();
						}else if(!tableField.value().equals("")){
							tableId = tableField.value();
						}
					} else {
						tableId = f.getName();
					}
				}
				if (tableId != null) {
					return tableId;
				}
			}
		}
		return null;
	}

	/**
	 * 获取主键id的值
	 * 
	 * @param <T>
	 */
	@SuppressWarnings("rawtypes")
	private <T> Object getTableIdValue(Object t) {
		for (Class clazz = t.getClass(); !clazz.equals(Object.class); clazz = clazz.getSuperclass()) {
			Field[] fs = clazz.getDeclaredFields();
			for (Field f : fs) {
				// 获取tableId注解
				TableId annotation = f.getDeclaredAnnotation(TableId.class);
				if (annotation != null) {
					f.setAccessible(true);
					try {
						return f.get(t);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			}
		}
		return null;
	}

	/**
	 * 获取原类属性id的名字
	 * 
	 * @param <T>
	 */
	@SuppressWarnings("rawtypes")
	private <T> String getTableIdJavaName(Object t) {
		for (Class clazz = t.getClass(); !clazz.equals(Object.class); clazz = clazz.getSuperclass()) {
			Field[] fs = clazz.getDeclaredFields();
			for (Field f : fs) {
				// 获取tableId注解
				TableId annotation = f.getDeclaredAnnotation(TableId.class);
				if (annotation != null) {
					return f.getName();
				}
			}
		}
		return null;
	}

	/**
	 * 获取对象中的所有属性
	 * 
	 * @param bean
	 *            对象
	 * @return 属性和值(Map[属性名, 属性值])
	 */
	@SuppressWarnings("rawtypes")
	private Map<String, Object> getAttributes(Object bean) {
		try {
			Map<String, Object> map = new HashMap<String, Object>();
			// 主键id字段是否被改变
			boolean changeTableId = false;
			for (Class clazz = bean.getClass(); !clazz.equals(Object.class); clazz = clazz.getSuperclass()) {
				Field[] fs = clazz.getDeclaredFields();
				for (Field f : fs) {
					f.setAccessible(true);
					if (f.get(bean) == null || (changeTableId && f.getName().equals("id"))) {
						f.setAccessible(false);
						continue;
					}
					// 如果该字段被标注忽略，则不往数据库添加此字段
					TableIgnore tableIgnore = f.getDeclaredAnnotation(TableIgnore.class);
					if (tableIgnore != null) {
						continue;
					}
					// 如果该字段被标注，则不从basebean中取id
					TableId tableId = f.getDeclaredAnnotation(TableId.class);
					// 如果当前类里面属性含有自定义注解，则处理属性名
					// if (clazz == bean.getClass()) {
					TableField tableField = f.getDeclaredAnnotation(TableField.class);
					if (tableId != null && !tableField.name().equals("")) {
						changeTableId = true;
					}
					if (tableField != null) {
						String tableFieldName = "";
						if(!tableField.name().equals("")){
							tableFieldName = tableField.name();
						}
						if(!tableField.value().equals("")){
							tableFieldName = tableField.value();
						}
						if(tableFieldName.equals("")){
							continue;
						}
						f.setAccessible(true);
						map.put(tableFieldName, f.get(bean));
						f.setAccessible(false);
						continue;
					}
					// }
					// 子类最大，父类值不覆盖子类
					if (map.containsKey(f.getName())) {
						continue;
					}
					f.setAccessible(true);
					Object value = f.get(bean);
					f.setAccessible(false);
					if (value == null) {
						continue;
					}
					if (!isBaseClass(value)) {
						if (value instanceof Page) {
							continue;
						}
						throw new RuntimeException("参数中不能嵌套非基本类");
					}
					map.put(f.getName(), value);
				}
			}
			map.remove("serialVersionUID");
			return map;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/***
	 * 校验是否是九种基础类型(即：非用户定义的类型)
	 * 
	 * @param value
	 *            字符串的值 要校验的值
	 * @return 是否是基础类型(true:已经是基础类型了)
	 */
	private boolean isBaseClass(Object value) {
		if (value == null) {
			return true;
		} else if (value instanceof Long) {
			return true;
		} else if (value instanceof Integer) {
			return true;
		} else if (value instanceof Double) {
			return true;
		} else if (value instanceof Float) {
			return true;
		} else if (value instanceof Byte) {
			return true;
		} else if (value instanceof Boolean) {
			return true;
		} else if (value instanceof Short) {
			return true;
		} else if (value instanceof Character) {
			return true;
		} else if (value instanceof String) {
			return true;
		}
		return false;
	}
	
	/***
	 * 校验是否是九种基础类型(即：非用户定义的类型)
	 * 
	 * @param value
	 *            字符串的值 要校验的值
	 * @return 是否是基础类型(true:已经是基础类型了)
	 */
	private boolean isBaseClass(@SuppressWarnings("rawtypes") Class value) {
		if (value == null) {
			return true;
		} else if (value.equals(Long.class)) {
			return true;
		} else if (value.equals(Integer.class)) {
			return true;
		} else if (value.equals(Double.class)) {
			return true;
		} else if (value.equals(Float.class)) {
			return true;
		} else if (value.equals(Byte.class)) {
			return true;
		} else if (value.equals(Boolean.class)) {
			return true;
		} else if (value.equals(Short.class)) {
			return true;
		} else if (value.equals(Character.class)) {
			return true;
		} else if (value.equals(String.class)) {
			return true;
		}
		return false;
	}
	
	/**
	 * 将map转成bean
	 * @param list
	 * @param obj
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <T> List<T> mapToBean(List<Map<String, Object>> list,T obj){
		// 最终封装后的返回
		List<T> resulTs = new ArrayList<T>();
		try {
			for (Map<String, Object> temp : list) {
				// 每条结果都封装在新obj对象里
				Object t = obj.getClass().newInstance();
				for (Class clazz = t.getClass(); !clazz.equals(Object.class); clazz = clazz.getSuperclass()) {
					Field[] fields = clazz.getDeclaredFields();
					for (Field f : fields) {
						//如果不是基本数据类型，则跳过本字段的赋值
						if(!isBaseClass(f.getType())){
							continue;
						}
						// 如果该类的属性名与表里不一致，则取表字段名
						TableField tableField = f.getDeclaredAnnotation(TableField.class);
						try {
							if (tableField != null) {
								String tableFieldName = "";
								if(!tableField.name().equals("")){
									tableFieldName = tableField.name();
								}
								if(!tableField.value().equals("")){
									tableFieldName = tableField.value();
								}
								if(tableFieldName.equals("")){
									throw new RuntimeException("类属性" + f.getName() + "与表字段类型不符");
								}
								f.setAccessible(true);
								f.set(t, temp.get(tableFieldName));
								f.setAccessible(false);
							}
							Class<?> type = f.getType();
							if(!isBaseClass(type)){
								continue;
							}
							if (temp.get(f.getName()) != null) {
								f.setAccessible(true);
								f.set(t, temp.get(f.getName()));
								f.setAccessible(false);
							}
						} catch (Exception e) {
							throw new RuntimeException("类属性" + f.getName() + "与表字段类型不符", e);
						}
					}
				}
				resulTs.add((T) t);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return resulTs;
	}
	/**
	 * 将map转成bean
	 * @param list
	 * @param clazz
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <T> List<T> mapToBean(List<Map<String, Object>> list,Class<T> clazz){
		try {
			if(list.size()==0){
				return null;
			}
			if(isBaseClass(clazz)){
				if(list.size()>0 && list.get(0).size()==1){
					List<T> result = new ArrayList<T>();
					T value = null;
					String mapKey = null;
					for (String key : list.get(0).keySet()) {
						mapKey = key;
						value = (T) list.get(0).get(key);
					}
					for (Map<String, Object> map : list) {
						value = (T) map.get(mapKey);
						result.add(value);
					}
					return result;
				}
			}
			T t = clazz.newInstance();
			return mapToBean(list, t);
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
}
