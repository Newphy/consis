package cn.newphy.consis.mq;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.ConnectionFactory;

import org.springframework.jms.listener.DefaultMessageListenerContainer;

public class MQMessageListener {

	private DefaultMessageListenerContainer messageListenerContainer;

	private Object messageListener;

	private String destination;

	public MQMessageListener(ConnectionFactory connectionFactory, String destination, Object messageListener) {
		this.messageListener = messageListener;
		messageListenerContainer = new DefaultMessageListenerContainer();
		messageListenerContainer.setConnectionFactory(connectionFactory);
		messageListenerContainer.setAutoStartup(false);
		messageListenerContainer.setDestinationName(destination);
		messageListenerContainer.setSessionTransacted(true);
		messageListenerContainer.setMessageListener(messageListener);
		messageListenerContainer.afterPropertiesSet();
	}

	public void start() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				doStart();
			}
		}).start();
	}

	public void start(final ReadyCondition condition) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				if (condition.isReady()) {
					doStart();
				}
			}
		}).start();
	}
	
	
	private synchronized void doStart() {
		if (messageListenerContainer != null && !messageListenerContainer.isRunning()) {
			messageListenerContainer.start();
		}		
	}
	
	
	public void close() {
		if(messageListenerContainer != null) {
			messageListenerContainer.shutdown();
		}
	}
	
	
	public boolean isStarted() {
		if(messageListenerContainer != null && messageListenerContainer.isActive()) {
			return messageListenerContainer.isRunning();
		}
		return false;
	}


	/**
	 * @return the messageListener
	 */
	public Object getMessageListener() {
		return messageListener;
	}
	
	

	/**
	 * @return the destination
	 */
	public String getDestination() {
		return destination;
	}

	public static class ReadyCondition {
		private volatile boolean ready = false;

		private Lock lock = new ReentrantLock();
		private Condition condition = lock.newCondition();

		public boolean isReady() {
			try {
				lock.lock();
				while (!ready) {
					condition.await();
				}
			} catch (InterruptedException e) {
			} finally {
				lock.unlock();
			}
			return ready;
		}

		public void ready() {
			try {
				lock.lock();
				ready = true;
				condition.signalAll();
			} finally {
				lock.unlock();
			}
		}
	}
}
