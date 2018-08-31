package cn.newphy.consis;

/**
 * 确认级别
 * 
 * @author Newphy
 * @time 2016年10月31日
 * @Copyright (c) www.newphy.cn
 */
public enum ConfirmLevel  {
	/**
	 * 缺省状态，默认为SENT级别
	 */
	DEFAULT,
	
	/**
	 * 保证发送成功
	 */
	SENT,
	
	/**
	 * 保证执行成功
	 */
	EXECUTED;
	
}
