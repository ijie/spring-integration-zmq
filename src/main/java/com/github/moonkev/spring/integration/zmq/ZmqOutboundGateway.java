package com.github.moonkev.spring.integration.zmq;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.Lifecycle;
import org.springframework.core.convert.converter.Converter;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.util.Assert;
import org.zeromq.ZMQ.Socket;

public class ZmqOutboundGateway extends AbstractReplyProducingMessageHandler implements Lifecycle, ZmqContextShutdownListener {

	protected final ExecutorService executorService = Executors.newSingleThreadExecutor();
	
	private ZmqContextManager contextManager;
	
	private volatile boolean running = false;
	
	private Socket socket;
	
	private String address;
	
	private boolean bind;
	
	protected final Object lifecycleMonitor = new Object();
	
	private Converter<Object, byte[]> requestConverter;
	
	private Converter<byte[], Object> replyConverter;
	
	private int socketType;
	
	private int socketSendTimeout;
	
	private int socketReceiveTimeout = 10000;
	
	private int linger = 0;
	
	protected Object handleRequestMessage(final Message<?> requestMessage) {
		
		if (!running) {
			return null;
		}
		
		Future<Object> response = executorService.submit(new Callable<Object>() {
			public Object call() throws Exception {
				byte[] requestData = requestConverter.convert(requestMessage.getPayload());
				socket.send(requestData, 0);
				byte[] replyData = socket.recv();
				if (replyData == null) {
					socket.close();
					ZmqOutboundGateway.this.connect();
				}
				return replyConverter.convert(replyData);
			}
		});
				
		try {
			return response.get();
		} catch (Throwable t) {
			throw new MessageHandlingException(requestMessage, t);
		}
	}
	
	public boolean isRunning() {
		return running;
	}
	
	protected void doInit() {
		super.doInit();
		Assert.notNull(socketType, "You must provide a socket type");
		Assert.notNull(address, "You must provide a valid ZMQ address");
		Assert.notNull(requestConverter, "You must provide a requestConverter");
		Assert.notNull(replyConverter, "You must provide a replyConverter");
	}
	
	public void connect() {
		socket = contextManager.context().socket(socketType);
		socket.setSendTimeOut(socketSendTimeout);
		socket.setReceiveTimeOut(socketReceiveTimeout);
		socket.setLinger(linger);
		if (bind) {
			socket.bind(address);
		} else {
			socket.connect(address);
		}
	}
	
	public void start() {
		synchronized (lifecycleMonitor) {
			if (!running) {
				Future<Void> response = executorService.submit(new Callable<Void>() {
					public Void call() throws Exception {
						ZmqOutboundGateway.this.connect();
						return null;
					}
				});
				try {
					response.get();
				} catch (Throwable t) {
					throw new BeanCreationException("Error starting zmq outbound gateway", t);
				}
				contextManager.registerShutdownListener(this);
				running = true;
			}
		}
	}
	
	public void stop() {
		synchronized (lifecycleMonitor) {
			if (running) {
				running = false;
				Future<Void> response = executorService.submit(new Callable<Void>() {
					public Void call() throws Exception {
						socket.close();
						return null;
					}
				});
				try {
					response.get();
					executorService.shutdown();
				} catch (Throwable t) {
					logger.error("Error shutting down socket in zmq outbound gateway");
				}
			}
		}
	}
	
	public void shutdownZmq() {
		this.stop();
	}
	
	public void setAddress(String address) {
		this.address = address;
	}
	
	public void setBind(boolean bind) {
		this.bind = bind;
	}
	
	public void setContextManager(ZmqContextManager contextManager) {
		this.contextManager = contextManager;
	}
	
	public void setSocketType(String socketTypeName) {
		this.socketType = ZmqEndpointUtil.socketTypeFromName(socketTypeName);
	}
	
	public void setRequestConverter(Converter<Object, byte[]> requestConverter) {
		this.requestConverter = requestConverter;
	}
	
	public void setReplyConverter(Converter<byte[], Object> replyConverter) {
		this.replyConverter = replyConverter;
	}
	
	public void setReplyChannel(MessageChannel replyChannel) {
		this.setOutputChannel(replyChannel);
	}
	
	public void setSocketSendTimeout(int socketSendTimeout) {
		this.socketSendTimeout = socketSendTimeout;
	}
	
	public void setSocketReceiveTimeout(int socketReceiveTimeout) {
		this.socketReceiveTimeout = socketReceiveTimeout;
	}
	
	public void setLinger(int linger) {
		this.linger = linger;
	}
}
