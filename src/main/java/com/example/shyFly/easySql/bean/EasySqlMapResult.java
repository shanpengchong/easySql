package com.example.shyFly.easySql.bean;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 分页查询时的返回对象,内含两个属性
 * 1:resultList<T> 查询的结果集合
 * 2:page : 分页的相关信息
 * @author spc
 * @param <T>
 *
 */
public class EasySqlMapResult implements Serializable{

	private static final long serialVersionUID = 1L;

	private List<Map<String,Object>> resultList;
	
	private Page page;

	/** 获取查询的结果集*/
	public List<Map<String,Object>> getResultList() {
		return resultList;
	}

	public void setResultList(List<Map<String,Object>> resultList) {
		this.resultList = resultList;
	}

	/** 获取分页对象*/
	public Page getPage() {
		return page;
	}

	public void setPage(Page page) {
		this.page = page;
	}
}
