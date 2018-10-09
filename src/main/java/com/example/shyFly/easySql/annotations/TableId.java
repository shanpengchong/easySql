package com.example.shyFly.easySql.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注表主键名称
 * 如果名字为id，则不用标记
 * @author spc
 *
 */
@Target({ElementType.FIELD})    
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TableId {
	/** 与数据库不同的字段*/
	
}
