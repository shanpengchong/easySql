package com.example.shyFly.easySql.bean;

/**
 * 操作符号
 * @author spc
 *
 */
public enum OperMark {

	lte(" <= "),
	gte(" >= "),
	equals(" = "),
	notEquals(" != "),
	in(" in "),
	notIn(" not in  "),
	like(" like ");
	private String remark;

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}
	private OperMark(String remark){
		this.remark = remark;
	}
}
