package cn.newphy.consis.handler;

import cn.newphy.consis.ConfirmLevel;
import cn.newphy.consis.ConsistencyInfo;

public interface ConsistencyHandler {

	/**
	 * 一致性处理
	 * 
	 * @param destination
	 * @param cobj
	 */
	void handle(String destination, ConsistencyObject cobj);

	/**
	 * 一致性处理
	 * 
	 * @param destination
	 * @param cobj
	 * @param confirmLevel
	 */
	void handle(String destination, ConsistencyObject cobj, ConfirmLevel confirmLevel);

	/**
	 * 一致性处理
	 * 
	 * @param destination
	 * @param obj
	 */
	void handle(String destination, Object obj);

	/**
	 * 一致性处理
	 * 
	 * @param destination
	 * @param obj
	 * @param confirmLevel
	 */
	void handle(String destination, Object obj, ConfirmLevel confirmLevel);

	/**
	 * 一致性处理
	 * 
	 * @param consistencyInfo
	 */
	void handle(ConsistencyInfo consistencyInfo);

	/**
	 * 补偿操作
	 * 
	 * @param maxCount
	 * @return 补偿数量
	 */
	int compensate(int maxCount);

}
