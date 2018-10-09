package com.example.shyFly.easySql.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
/**
 * 插入数据库时需要忽略的字段
 * 标注后该字段不会被插入数据库
 * @author spc
 *
 */
@Target({ElementType.FIELD, ElementType.TYPE})    
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TableIgnore {

}
