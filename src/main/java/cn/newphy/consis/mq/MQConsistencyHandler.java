package cn.newphy.consis.mq;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.List;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.alibaba.fastjson.JSON;

import cn.newphy.consis.ConfirmLevel;
import cn.newphy.consis.ConfirmMessage;
import cn.newphy.consis.ConfirmStatus;
import cn.newphy.consis.ConsistencyInfo;
import cn.newphy.consis.DateUtils;
import cn.newphy.consis.RetryStatus;
import cn.newphy.consis.handler.ConsistencyHandlerSupport;
import cn.newphy.consis.handler.ConsistencyObject;
import cn.newphy.consis.support.transaction.TransactionSynchronizationUtil;

public class MQConsistencyHandler extends ConsistencyHandlerSupport implements DisposableBean {
	private Logger logger = LoggerFactory.getLogger(MQConsistencyHandler.class);

	private static final int DEFAULT_RETRY_INTERVAL = 10 * 60;
	// 最大重试次数和重试过期时间
	private static final int MAX_RETRY_COUNT = 100;
	private static final long MAX_RETRY_OVERDUE = 10*24*3600*1000L;

	private JmsTemplate jmsTemplate;

	private ConnectionFactory connectionFactory;

	private String confirmDestination;
	
	private MQMessageListener mqMessageListener;
	
	private volatile boolean started = false;

	@Override
	public void handle(String destination, ConsistencyObject cobj) {
		this.handle(destination, cobj, ConfirmLevel.SENT);
	}
	
	

	@Override
	public void handle(String destination, ConsistencyObject cobj, ConfirmLevel confirmLevel) {
		if(cobj.getObject() ==  null) {
			throw new NullPointerException("一致性对象cobj为空");
		}
		ConsistencyInfo cinfo = createConsistencyInfo(destination, cobj);
		if(confirmLevel != null) {
			cinfo.setConfirmLevel(confirmLevel);
		}
		handle(cinfo);
	}

	

	@Override
	public void handle(String destination, Object obj) {
		this.handle(destination, obj, ConfirmLevel.SENT);
	}


	@Override
	public void handle(String destination, Object obj, ConfirmLevel confirmLevel) {
		if(obj == null) {
			throw new NullPointerException("一致性对象obj为空");
		}
		this.handle(destination, new IdConsistencyObject(obj), confirmLevel);		
	}



	@Override
	public void handle(final ConsistencyInfo cinfo) {
		// 保存消息
		Date firstTime = new Date();
		cinfo.setFirstSentTime(firstTime);
		cinfo.setRetryCount(0);
		cinfo.setRetryStatus(RetryStatus.YES);
		cinfo.setRetryTime(DateUtils.addSeconds(new Date(), cinfo.getRetryInterval()));
		cinfo.setConfirmDestination(confirmDestination);		
		cinfo.setConfirmStatus(ConfirmStatus.INTIAL);
		consistencyDao.addConsistency(cinfo);
		// 发送消息
		TransactionSynchronizationUtil.registerSynchronization(new TransactionSynchronizationAdapter() {
			@Override
			public void afterCommit() {
				sendMessage(cinfo);
			}
		});
	}

	@Override
	public int compensate(int maxCount) {
		if(started) {
			List<ConsistencyInfo> messages = consistencyDao.queryRetryList(maxCount);
			int success = 0;
			for (ConsistencyInfo message : messages) {
				ConsistencyInfo detail = consistencyDao.getDetail(message.getId());
				success += sendMessage(detail) ? 1 : 0;
			}
			return success;
		}
		return -1;
	}
	
	
	/**
	 * 增加重试策略
	 * <p>
	 * <ul>
	 * <li>第一阶段: < 10, 按设置时间发送  </li>
	 *  <li>第二阶段: 10<= retryCount < 20,  最小600s间隔 </li>
	 *  <li> 第三阶段: 20 <= retryCount <30, 最小1800s间隔 </li>
	 *  <li>第四阶段: 30 <= retryCount <MaxRetryCount, 最小3600s间隔 </li>
	 *  <li>最高限制: 10天或100次 </li>
	 * </p>
	 * 
	 * @param consistencyInfo
	 * @return
	 */
	private boolean sendMessage(ConsistencyInfo consistencyInfo) {
		int retryCount = consistencyInfo.getRetryCount();
		int retryInterval = consistencyInfo.getRetryInterval();
		Date firstTime = consistencyInfo.getFirstSentTime();
		// 超过次数和超时都不再发送
		if(retryCount == MAX_RETRY_COUNT || System.currentTimeMillis() - firstTime.getTime() > MAX_RETRY_OVERDUE) {
			consistencyInfo.setRetryStatus(RetryStatus.NO);
			consistencyInfo.setFailCause("超过最大过期时间和重试次数");
			consistencyDao.updateConsistency(consistencyInfo);
			return false;
		}
		// 发送消息
		String failCause = doSendMessage(consistencyInfo);
		boolean successful = false;
		// 成功
		if(StringUtils.isEmpty(failCause)) {
			// 更新确认发送消息
			consistencyInfo.setConfirmSentTime(new Date());
			if (consistencyInfo.getConfirmStatus() == ConfirmStatus.INTIAL) {
				consistencyInfo.setConfirmStatus(ConfirmStatus.SENT);
			}
			// 判断是否需要重新发送
			consistencyInfo.setRetryStatus((consistencyInfo.getConfirmLevel() == ConfirmLevel.SENT) ? RetryStatus.NO : RetryStatus.YES);
			consistencyInfo.setFailCause("");
			successful = true;
		}
		// 失败
		else {
			consistencyInfo.setFailCause(failCause);
			successful = false;
		}
		
		// 计算下次发送时间，阶梯策略
		if(retryCount >= 10 && retryCount < 20) {
			retryInterval = Math.max(retryInterval, 600);
		} else if(retryCount >= 20 && retryCount < 30) {
			retryInterval = Math.max(retryInterval, 1800);
		} else if(retryCount >= 30) {
			retryInterval = Math.max(retryInterval, 3600);
		}
		consistencyInfo.setRetryTime(DateUtils.addSeconds(new Date(), retryInterval));
		consistencyInfo.setRetryCount(retryCount+1);
		consistencyDao.updateConsistency(consistencyInfo);
		return successful;
	}
	
	private String doSendMessage(final ConsistencyInfo cinfo) {
		logger.info("~~~ 发送一致性消息, txId={}, cinfo={} ~~~", cinfo.getTxId(), cinfo);
		try {
			MessageCreator messageCreator = new MessageCreator() {
				public Message createMessage(Session session) throws JMSException {
					TextMessage textMessage = session.createTextMessage();
					textMessage.setText(cinfo.getContent());
					textMessage.setStringProperty(MQKeys.TX_ID, cinfo.getTxId());
					textMessage.setIntProperty(MQKeys.CONFIRM_LEVEL, cinfo.getConfirmLevel().ordinal());
					// 判断是否需要回复
					if (cinfo.getConfirmLevel() == ConfirmLevel.EXECUTED) {
						if(StringUtils.isEmpty(confirmDestination)) {
							throw new IllegalStateException("没有设置确认地址confirmDestination");
						}
						Destination destination = jmsTemplate.getDestinationResolver().resolveDestinationName(session,
								confirmDestination, false);
						textMessage.setJMSReplyTo(destination);
					}
					return textMessage;
				}
			};
			// Queue
			jmsTemplate.send(cinfo.getDestination(), messageCreator);
			logger.info("~~~ 发送一致性消息成功, txId={}, message={} ~~~", cinfo.getTxId(), cinfo);
			return "";
		} catch (Exception e) {
			logger.error("~~~ 发送一致性消息出错, txId={}, message={} ~~~", cinfo.getTxId(), cinfo, e);
			Throwable t = e;
			if(t instanceof InvocationTargetException){
				t = ((InvocationTargetException) t).getTargetException();
			}
			String error = t == null ? e.getMessage() : t.getMessage();
			return error.substring(0, 100);
		}
	}
	

	private ConsistencyInfo createConsistencyInfo(String destination, ConsistencyObject cobj) {
		ConsistencyInfo cinfo = new ConsistencyInfo();
		cinfo.setDestination(destination);
		cinfo.setConfirmLevel(ConfirmLevel.SENT);
		cinfo.setRetryInterval(DEFAULT_RETRY_INTERVAL);
		cinfo.setBizId(cobj.getObjectId());
		cinfo.setConfirmDestination(this.confirmDestination);
		
		Object bizObj = cobj.getObject();
		cinfo.setContent(JSON.toJSONString(bizObj));
		return cinfo;
	}



	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		Assert.isTrue(StringUtils.hasText(confirmDestination), "没有设置确认队列confirmDestination");

		if (jmsTemplate == null && connectionFactory == null) {
			throw new IllegalStateException("没有设置消息队列ConnectionFactory");
		}
		if (jmsTemplate == null) {
			this.jmsTemplate = new JmsTemplate(this.connectionFactory);
		}
		if(StringUtils.hasText(confirmDestination)) {
			createConfirmMessageListener();
		}
		started = true;
	}

	private void createConfirmMessageListener() {
		if(mqMessageListener != null) {
			return;
		}
		MessageListener messageListener = new MessageListener() {
			@Override
			public void onMessage(Message message) {
				try {
					TextMessage txtMessage = (TextMessage) message;
					String replyJson = txtMessage.getText();
					logger.info("~~~ 收到一致性确认消息, confirmJson={}", replyJson);
					ConfirmMessage crm = JSON.parseObject(replyJson, ConfirmMessage.class);
					if(crm.isSuccess()) {
						String txId = crm.getTxId();
						ConsistencyInfo cinfo = consistencyDao.getConsistencyByTxId(txId);
						cinfo.setConfirmStatus(ConfirmStatus.EXECUTED);
						cinfo.setConfirmExecuteTime(crm.getExecuteTime());
						cinfo.setExecuteHost(crm.getExecuteHost());
						cinfo.setRetryStatus(RetryStatus.NO);
						consistencyDao.updateConsistency(cinfo);
					}
				} catch (JMSException e) {
					logger.warn("~~~ 收到一致性确认消息出错 ~~~", e);
				}
			}
		};
		mqMessageListener = new MQMessageListener(getConnectionFactory(), confirmDestination, messageListener);
		mqMessageListener.start();
	}

	

	@Override
	public void destroy() throws Exception {
		if(mqMessageListener != null && mqMessageListener.isStarted()) {
			mqMessageListener.close();
		}
	}



	/**
	 * @param dataSource
	 *            the dataSource to set
	 */
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	/**
	 * @return the connectionFactory
	 */
	public ConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	/**
	 * @param connectionFactory
	 *            the connectionFactory to set
	 */
	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @return the confirmDestination
	 */
	public String getConfirmDestination() {
		return confirmDestination;
	}

	/**
	 * @param confirmDestination
	 *            the confirmDestination to set
	 */
	public void setConfirmDestination(String confirmDestination) {
		this.confirmDestination = confirmDestination;
	}

	

}
