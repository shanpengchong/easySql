package com.example.shyFly.easySql.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * 拼接类
 * 
 * @author spc
 *
 */
public class SpliceBean {

	private String spliceSql;
	private List<Object> spliceParam = new ArrayList<Object>();

	/** 拼接一段聚合查询 */
	public SpliceBean(String operTableField, OperMark om, Object value) {
		if (operTableField == null) {
			throw new RuntimeException("要操作聚合的表字段不能为空");
		}
		if (om == null) {
			throw new RuntimeException("要操作聚合的函数不能为空");
		}
		if (value == null) {
			throw new RuntimeException("要操作聚合的表字段的值不能为空");
		}
		if (om.equals(OperMark.like)) {
//			this.spliceSql = " and " + operTableField + " " + om.getRemark() + " '%" + value + "%' ";
			this.spliceSql = " and " + operTableField + om.getRemark() + "? ";
			this.spliceParam.add("%"+value+"%");
		} else if (om.equals(OperMark.in) || om.equals(OperMark.notIn)) {
			if (value instanceof java.util.List) {
				@SuppressWarnings("unchecked")
				List<Object> list = (List<Object>) value;
				StringBuffer str = new StringBuffer();
				str.append(" and " + operTableField + " " + om.getRemark() + " ( ");
				for (Object o : list) {
					str.append("?,");
					this.spliceParam.add(o);
				}
				this.spliceSql = str.deleteCharAt(str.length()-1).append(" )").toString();
			}else{
				throw new RuntimeException("参数非集合");
			}
			
		} else {
			this.spliceSql = " and " + operTableField + " " + om.getRemark() + " ? ";
			this.spliceParam.add(value);
		}
	}

	public SpliceBean() {
		this.spliceSql = "";
	}

	public String getSpliceSql() {
		return spliceSql;
	}

	public List<Object> getSpliceParam() {
		return spliceParam;
	}
}
