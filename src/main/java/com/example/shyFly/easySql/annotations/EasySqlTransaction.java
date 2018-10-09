package com.example.shyFly.easySql.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标注本方法下的所有方法执行事务
 * @author spc
 *
 */
@Target({ElementType.METHOD})    
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EasySqlTransaction {

}
