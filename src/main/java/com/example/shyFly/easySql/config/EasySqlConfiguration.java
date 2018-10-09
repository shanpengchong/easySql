package com.example.shyFly.easySql.config;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.druid.pool.DruidDataSource;

public class EasySqlConfiguration {
	public static DataSource dataSource;
	private  static Map<String, String> propers = new HashMap<String, String>();
	/** 多数据源存储*/
	private static ConcurrentHashMap<String, DataSource> dataSourceMap = new ConcurrentHashMap<String, DataSource>();
	public static String url;
	public static String username;
	public static String password;
	
	public static DataSource getDataSourceMapValue(String flag){
		return dataSourceMap.get(flag);
	}
	public static void setDataSourceMapValue(String flag,DataSource dataSource){
		if(dataSource == null){
			dataSourceMap.put(flag, new EasySqlConfiguration().dataSource());
		}else{
			dataSourceMap.put(flag, dataSource);
		}
	}
	public  DruidDataSource dataSource() {
		if(url!=null && username!=null && password!=null){
			propers.put("url", url);
			propers.put("username", username);
			propers.put("password", password);
		}else{
			// 获取项目路径
//			String project_path = System.getProperty("user.dir");
			Properties propFlies = new Properties();
			try {
				propFlies.load(this.getClass().getResourceAsStream("/application.properties"));
				for (Object key : propFlies.keySet()) {
					hasMySqlConfig(key.toString(), propFlies.get(key).toString());
				}
//				File file = new File(project_path);
//				listFile(file);
				if(propers.get("url")==null || propers.get("username")==null || propers.get("password")==null){
					throw new RuntimeException("未能找到您项目中perproties文件中数据库连接配置");
				}
				LoggerUtil.info(this.getClass(),"读取到配置文件的username=" + propers.get("username"));
				LoggerUtil.info(this.getClass(),"读取到配置文件的password=" + propers.get("password"));
				LoggerUtil.info(this.getClass(),"读取到配置文件的url=" + propers.get("url"));
			} catch (Exception e) {
				throw new RuntimeException("未能找到您项目中application.properties文件中数据库连接配置");
			}
		}
		DruidDataSource dataSource = new DruidDataSource();
		dataSource.setUrl(propers.get("url"));
		// 用户名
		dataSource.setUsername(propers.get("username"));
		// 密码
		dataSource.setPassword(propers.get("password"));
		dataSource.setInitialSize(2);
		dataSource.setMaxActive(20);
		dataSource.setMinIdle(0);
		dataSource.setMaxWait(60000);
		dataSource.setValidationQuery("SELECT 1");
		dataSource.setTestOnBorrow(false);
		dataSource.setTestWhileIdle(true);
		dataSource.setPoolPreparedStatements(false);
		Properties properties = dataSource.getConnectProperties();
		properties.put("autoReconnect", "true");
		dataSource.setConnectProperties(properties);
		return dataSource;
	}

	public  void listFile(File file) {
		if(propers.get("url")!=null && propers.get("username")!=null && propers.get("password")!=null){
			return;
		}
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (File f : files) {
				if (f.isFile() && f.getName().endsWith(".properties")) {
					Properties properties = new Properties();
					BufferedReader bufferedReader = null;
					try {
						bufferedReader = new BufferedReader(new FileReader(f.getAbsolutePath()));
						properties.load(bufferedReader);
						Iterator<Entry<Object, Object>> iterator = properties.entrySet().iterator();
						while (iterator.hasNext()) {
							Entry<Object, Object> next = iterator.next();
							String key = next.getKey().toString();
							hasMySqlConfig(key, next.getValue().toString());
						}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if(bufferedReader != null) {
							try {
								bufferedReader.close();
								return;
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						}
					}
				}
				if (f.isDirectory()) {
					listFile(f);
				}
			}
		}
	}
	
	private  void hasMySqlConfig(String key,String value){
		key = key.toUpperCase();
		// 获取username
		if (key.contains("MYSQL") && key.contains("USER") && key.contains("NAME")) {
			propers.put("username", value);
		} else if (key.contains("DATASOURCE") && key.contains("USER") && key.contains("NAME")) {
			propers.put("username", value);
		} else if (key.contains("JDBC") && key.contains("USER") && key.contains("NAME")) {
			propers.put("username", value);
		}
		// 获取password
		if (key.contains("MYSQL") && (key.contains("PASSWORD") || key.contains("PWD"))) {
			propers.put("password", value);
		} else if (key.contains("DATASOURCE")
				&& (key.contains("PASSWORD") || key.contains("PWD"))) {
			propers.put("password", value);
		} else if (key.contains("JDBC") && (key.contains("PASSWORD") || key.contains("PWD"))) {
			propers.put("password", value);
		}
		// 获取url
		if (key.contains("MYSQL") && key.contains("URL")) {
			propers.put("url", value);
		} else if (key.contains("DATASOURCE") && key.contains("URL")) {
			propers.put("url", value);
		} else if (key.contains("JDBC") && key.contains("URL")) {
			propers.put("url", value);
		}
		if(propers.get("url")!=null && propers.get("username")!=null && propers.get("password")!=null){
//			propers.put("fileName", f.getName());
			return;
		}
	}
}
