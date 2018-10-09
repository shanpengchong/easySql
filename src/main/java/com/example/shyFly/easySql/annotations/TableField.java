package com.example.shyFly.easySql.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注数据库表的字段名称
 * 如果与本地名称相同，则不用标记
 * @author spc
 *
 */
@Target({ElementType.FIELD})    
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TableField {

	/** 与数据库不同的字段*/
	String value()default"";
	String name()default"";
}
