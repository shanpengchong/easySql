package com.example.shyFly.easySql.config;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.sql.Connection;

import com.example.shyFly.easySql.annotations.EasySqlTransaction;

public class TransactionUtil {
	/** 当前线程获取到的连接*/
	public static ThreadLocal<Connection> localConnection = new ThreadLocal<Connection>();
	/** 是否需要开启事务*/
	public static ThreadLocal<Boolean> isTransaction = new ThreadLocal<Boolean>();
	/** 用户设置提交方式*/
	public static ThreadLocal<Boolean> autoCommit = new ThreadLocal<Boolean>();

	/** 查询本方法是否开启了事务*/
	public boolean isTranMothed(){
		Boolean userSet = autoCommit.get();
		if(userSet!=null && !userSet){
			isTransaction.set(true);
			return true;
		}
		StackTraceElement[] s = new Exception().getStackTrace();
		for (StackTraceElement stackTraceElement : s) {
			String className = stackTraceElement.getClassName();
			String mothedName = stackTraceElement.getMethodName();
			try {
				Class<?> clazz = Class.forName(className);
				Method[] methods = clazz.getDeclaredMethods();
				for (Method m : methods) {
					if(m.getName().equals(mothedName)){
						Annotation[] ans = m.getDeclaredAnnotations();
						for (Annotation an : ans) {
							if(an instanceof EasySqlTransaction){
								return true;
							}
						}
						
					}
				}
			} catch (Exception e) {
				return false;
			}
		}
		return false;
	}
}
