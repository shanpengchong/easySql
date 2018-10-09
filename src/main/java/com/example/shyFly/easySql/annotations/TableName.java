package com.example.shyFly.easySql.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 标记数据库表名 */
@Target({ElementType.TYPE})    
@Retention(RetentionPolicy.RUNTIME)
@Documented 
public @interface TableName {

	String value()default"";
	String name()default"";
}
