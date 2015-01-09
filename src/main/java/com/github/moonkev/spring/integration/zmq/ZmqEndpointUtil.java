package com.github.moonkev.spring.integration.zmq;

import java.lang.reflect.Field;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.util.ReflectionUtils;
import org.zeromq.ZMQ;

public class ZmqEndpointUtil {

	
	public static int socketTypeFromName(String socketTypeName) {
		Field socketTypeField = ReflectionUtils.findField(ZMQ.class, socketTypeName);
		if (socketTypeField == null  || socketTypeField.getType() != int.class) {
			throw new BeanCreationException(String.format("%s is not a valid ZMQ socket type", socketTypeName));
		}
		return (Integer) ReflectionUtils.getField(socketTypeField, null);
	}
	
	public static MessageHandlingException buildMessageHandlingException(Message<?> failedMessage, int errno)  {
			return new MessageHandlingException(failedMessage, "ZMQ Context has been Exception errno:"+errno);
	}
}
