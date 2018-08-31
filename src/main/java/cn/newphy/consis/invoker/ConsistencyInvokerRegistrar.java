package cn.newphy.consis.invoker;

public interface ConsistencyInvokerRegistrar {

	/**
	 * 注册一致性Invoker
	 * 
	 * @param path
	 * @param invoker
	 */
	<T> void registerInvoker(String destination, ConsistencyInvoker<T> invoker);

	
	/**
	 * 开始
	 */
	void start();
}
