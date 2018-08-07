package com.ultrapower.bms.spark.entity;

import java.io.Serializable;
import java.util.Date;

/**
 * 日志
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
public class Log implements Serializable {

	/** SerialVersionUID */
	private static final long serialVersionUID = -4729268147101904923L;
	
	/** 登录用户名 */
	private String loginName;
	/** 资源类型 */
	private String resType;
	/** 登录结果 */
	private Integer loginResult;
	/** 登录时间  */
	private Date loginTime;
	
	public Log() {}
	
	public Log(String loginName,String resType,Integer loginResult,Date loginTime) {
		this.loginName = loginName;
		this.resType = resType;
		this.loginResult = loginResult;
		this.loginTime = loginTime;
	}
	
	public String getLoginName() {
		return loginName;
	}
	public void setLoginName(String loginName) {
		this.loginName = loginName;
	}
	public String getResType() {
		return resType;
	}
	public void setResType(String resType) {
		this.resType = resType;
	}
	public Integer getLoginResult() {
		return loginResult;
	}
	public void setLoginResult(Integer loginResult) {
		this.loginResult = loginResult;
	}
	public Date getLoginTime() {
		return loginTime;
	}
	public void setLoginTime(Date loginTime) {
		this.loginTime = loginTime;
	}
}
