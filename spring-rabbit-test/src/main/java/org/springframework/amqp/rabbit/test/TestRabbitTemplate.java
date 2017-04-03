/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.adapter.AbstractAdaptableMessageListener;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

/**
 * A {@link RabbitTemplate} that invokes {@code @RabbitListener} s directly.
 * It currently only supports the queue name in the routing key.
 * It does not currently support publisher confirms/returns.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public class TestRabbitTemplate extends RabbitTemplate implements ApplicationContextAware, SmartInitializingSingleton {

	private static final String REPLY_QUEUE = "testRabbitTemplateReplyTo";

	private ApplicationContext applicationContext;

	private boolean broadcast;

	@Autowired
	private RabbitListenerEndpointRegistry registry;

	private MessageListenerAccessor messageListenerAccessor;

	public TestRabbitTemplate(ConnectionFactory connectionFactory) {
		super(connectionFactory);
		setReplyAddress(REPLY_QUEUE);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	/**
	 * Set to true to broadcast send operations when there are multiple listeners on the
	 * same queue (simulate a fanout exchange). Otherwise, messages will send round-robin.
	 * Does not apply to send and receive operations.
	 * @param broadcast true to broadcast.
	 */
	public void setBroadcast(boolean broadcast) {
		this.broadcast = broadcast;
	}

	@Override
	public void afterSingletonsInstantiated() {
		Collection<AbstractMessageListenerContainer> abstractMessageListenerContainers = this.applicationContext.getBeansOfType(AbstractMessageListenerContainer.class).values();
		this.messageListenerAccessor = new MessageListenerAccessor(registry, abstractMessageListenerContainers, applicationContext.getBeansOfType(Binding.class).values());
	}


	@Override
	protected boolean useDirectReplyTo() {
		return false;
	}

	@Override
	protected void sendToRabbit(Channel channel, String exchange, String routingKey, boolean mandatory,
			Message message) throws IOException {
		List<AbstractAdaptableMessageListener> listeners = this.messageListenerAccessor.getListenerContainersForDestination(exchange);
		if (listeners.isEmpty()) {
			throw new IllegalArgumentException("No listener for " + routingKey);
		}
		try {
			invokeListener(listeners, message, channel);
		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	private void invokeListener(List<AbstractAdaptableMessageListener> listeners, final Message message, final Channel channel) {
		if (this.broadcast) {
			listeners.forEach(l -> invoke(l, message, channel));
		}
		else {
			invoke(listeners.iterator().next(), message, channel);
		}
	}

	@Override
	protected Message doSendAndReceiveWithFixed(String exchange, String routingKey, Message message,
			CorrelationData correlationData) {
		List<AbstractAdaptableMessageListener> listeners = this.messageListenerAccessor.getListenerContainersForDestination(exchange);

		if (listeners.isEmpty()) {
			throw new IllegalArgumentException("No listener for " + routingKey);
		}
		Channel channel = mock(Channel.class);
		final AtomicReference<Message> reply = new AtomicReference<>();
		Object listener = listeners.get(0);
		if (listener != null) {
			try {
				AbstractAdaptableMessageListener adapter = (AbstractAdaptableMessageListener) listener;
				willAnswer(i -> {
					Envelope envelope = new Envelope(1, false, "", REPLY_QUEUE);
					reply.set(MessageBuilder.withBody(i.getArgument(4))
							.andProperties(getMessagePropertiesConverter().toMessageProperties(i.getArgument(3), envelope,
									adapter.getEncoding()))
							.build());
					return null;
				}).given(channel).basicPublish(anyString(), anyString(), anyBoolean(), any(BasicProperties.class),
						any(byte[].class));
				message.getMessageProperties().setReplyTo(REPLY_QUEUE);
				adapter.onMessage(message, channel);
			}
			catch (Exception e) {
				throw RabbitExceptionTranslator.convertRabbitAccessException(e);
			}
		}
		else {
			throw new IllegalStateException("sendAndReceive not supported for " + listener.getClass().getName());
		}
		return reply.get();
	}

	private void invoke(Object listener, Message message, Channel channel) {
		if (listener instanceof ChannelAwareMessageListener) {
			try {
				((ChannelAwareMessageListener) listener).onMessage(message, channel);
			}
			catch (Exception e) {
				throw RabbitExceptionTranslator.convertRabbitAccessException(e);
			}
		}
		else if (listener instanceof MessageListener) {
			((MessageListener) listener).onMessage(message);
		}
	}

	private static class Listeners {

		private final List<Object> listeners = new ArrayList<>();

		private volatile Iterator<Object> iterator;

		Listeners(Object listener) {
			this.listeners.add(listener);
		}

		private Object next() {
			if (this.iterator == null || !this.iterator.hasNext()) {
				this.iterator = this.listeners.iterator();
			}
			return this.iterator.next();
		}
	}

}
