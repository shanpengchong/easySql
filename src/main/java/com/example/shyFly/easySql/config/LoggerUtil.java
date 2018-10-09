package com.example.shyFly.easySql.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 日志 */
public class LoggerUtil {
	/** 项目名 */
	private static String projectName = "--easySql--";
     
	
	/** 获取当前项目的名称 */
	public static String getProjectName() {
		return projectName;
	}
	
	private Class<?> clazz;
	public LoggerUtil(Class<?> clazz) {
		this.clazz = clazz;
	}
	
	public LoggerUtil() {
		Class<?> clazz = null;
		try {
			String className = Thread.currentThread().getStackTrace()[2].getClassName();
			clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		this.clazz = clazz;
	}

	/** 获取log对象 */
	public Logger getLogger() {
		return LoggerFactory.getLogger(clazz);
	}
	/** 获取log对象 */
	public static Logger getLogger(Class<?> clazz) {
		return LoggerFactory.getLogger(clazz);
	}

	public static void debug(Class<?> clazz, String str) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).debug("[" + projectName + "]" + str);
		
	}

	public static void debug(Class<?> clazz, String str, Object obj) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).debug("[" + projectName + "]" + str, obj);
		
	}

	public static void debug(Class<?> clazz, String str, Object[] objs) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).debug("[" + projectName + "]" + str, objs);
		
	}

	public static void debug(Class<?> clazz, Throwable e) {
		LoggerFactory.getLogger(clazz).debug("[" + projectName + "]", e);
	}

	public static void info(Class<?> clazz, String str) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).info("[" + projectName + "]" + str);
		
	}

	public static void info(Class<?> clazz, String str, Object obj) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).info("[" + projectName + "]" + str, obj);
		
	}

	public static void info(Class<?> clazz, String str, Object[] objs) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).info("[" + projectName + "]" + str, objs);
		
	}

	public static void error(Class<?> clazz, String str) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).error("[" + projectName + "]" + str);
		
	}

	public static void error(Class<?> clazz, String str, Object obj) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).error("[" + projectName + "]" + str, obj);
		
	}

	public static void error(Class<?> clazz, String str, Object[] objs) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).error("[" + projectName + "]" + str, objs);
		
	}

	public static void error(Class<?> clazz, String str, Throwable e) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).error("[" + projectName + "]" + str, e);
		
	}

	public static void error(Class<?> clazz, Throwable e) {
		error(clazz, "", e);
	}
	
	public void debug(String str) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).debug("[" + projectName + "]" + str);
		
	}

	public void debug(String str, Object obj) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).debug("[" + projectName + "]" + str, obj);
		
	}

	public void debug(String str, Object[] objs) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).debug("[" + projectName + "]" + str, objs);
		
	}

	public void debug(Throwable e) {
		LoggerFactory.getLogger(clazz).debug("[" + projectName + "]", e);
	}

	public void info(String str) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).info("[" + projectName + "]" + str);
		
	}

	public void info(String str, Object obj) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).info("[" + projectName + "]" + str, obj);
		
	}

	public void info(String str, Object[] objs) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).info("[" + projectName + "]" + str, objs);
		
	}

	public void error(String str) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).error("[" + projectName + "]" + str);
		
	}

	public void error(String str, Object obj) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).error("[" + projectName + "]" + str, obj);
		
	}

	public void error(String str, Object[] objs) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).error("[" + projectName + "]" + str, objs);
		
	}

	public void error(String str, Throwable e) {
		str = str == null ? "" : str;
		LoggerFactory.getLogger(clazz).error("[" + projectName + "]" + str, e);
		
	}

	public void error(Throwable e) {
		error(clazz, "", e);
	}
}
