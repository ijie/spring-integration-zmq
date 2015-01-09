package com.github.moonkev.spring.integration.zmq;

import java.util.Collection;
import java.util.HashSet;

import org.springframework.context.Lifecycle;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

public class ZmqContextManager implements Lifecycle {

	private  Context context;
		
	private volatile boolean running = false;
		
	private Collection<ZmqContextShutdownListener> shutdownListeners = new HashSet<ZmqContextShutdownListener>();
	
	public ZmqContextManager(int ioThreads) {
		context = ZMQ.context(ioThreads);
	}
	
	public Context context() {
		return context;
	}

	public void start() {
		running = true;
	}
	
	public void stop() {
		running = false;
		for (ZmqContextShutdownListener listener : shutdownListeners) {
			listener.shutdownZmq();
		}
		context.term();
	}
	
	public boolean isRunning() {
		return running;
	}
	
	public void registerShutdownListener(ZmqContextShutdownListener listener) {
		shutdownListeners.add(listener);
	}
}
