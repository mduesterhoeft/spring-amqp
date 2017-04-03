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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;


/**
 * @author Gary Russell
 * @since 2.0
 *
 */
@RunWith(SpringRunner.class)
public class TestRabbitTemplateTests {

	@Autowired
	private TestRabbitTemplate template;

	@Autowired
	private Config config;

	@Test
	public void testSimpleSends() {
		this.template.setBroadcast(true);
		this.template.convertAndSend("foo.exchange", "foo", "hello1");
		assertThat(this.config.fooIn, equalTo("foo:hello1"));
		assertThat(this.config.fooSimpleIn, equalTo("fooSimple:hello1"));

		this.template.convertAndSend("bar.exchange", "bar", "hello2");
		assertThat(this.config.barIn, equalTo("bar:hello2"));
		
	}

	@Test
	public void testSendAndReceive() {
		assertThat(this.template.convertSendAndReceive("baz.exchange", "baz", "hello"), equalTo("baz:hello"));
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		public String fooIn = "";

		public String fooSimpleIn;

		public String barIn = "";

		@Bean
		public TestRabbitTemplate template() throws IOException {
			return new TestRabbitTemplate(connectionFactory());
		}

		@Bean
		public ConnectionFactory connectionFactory() throws IOException {
			ConnectionFactory factory = mock(ConnectionFactory.class);
			Connection connection = mock(Connection.class);
			Channel channel = mock(Channel.class);
			willReturn(connection).given(factory).createConnection();
			willReturn(channel).given(connection).createChannel(anyBoolean());
			given(channel.isOpen()).willReturn(true);
			return factory;
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() throws IOException {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(connectionFactory());
			return factory;
		}

		@Bean
		org.springframework.amqp.core.Queue fooSimpleQueue() {
			return new org.springframework.amqp.core.Queue("fooSimple");
		}

		@Bean
		org.springframework.amqp.core.Exchange fooExchange() {
			return new TopicExchange("foo.exchange");
		}

		@Bean
		Binding fooBinding() {
			return BindingBuilder.bind(fooSimpleQueue()).to(fooExchange()).with("some").noargs();
		}


		@Bean
		public SimpleMessageListenerContainer simpleMessageListenerContainer(ConnectionFactory connectionFactory) {
			SimpleMessageListenerContainer simpleMessageListenerContainer = new SimpleMessageListenerContainer(connectionFactory);

			simpleMessageListenerContainer.addQueues(fooSimpleQueue());
			simpleMessageListenerContainer.setMessageListener(new MessageListenerAdapter((MessageListener) message -> {
				this.fooSimpleIn = "fooSimple:" + new String(message.getBody());
            }));
			return simpleMessageListenerContainer;
		}

		@RabbitListener(bindings = @QueueBinding(
				value = @Queue(name = "foo"),
				exchange = @Exchange(name = "foo.exchange"))
		)
		public void foo(String in) {
			this.fooIn += "foo:" + in;
		}

		@RabbitListener(bindings = @QueueBinding(
				value = @Queue(name = "bar"),
				exchange = @Exchange(name = "bar.exchange"))
		)
		public void bar(String in) {
			this.barIn += "bar:" + in;
		}

		@RabbitListener(bindings = @QueueBinding(
				value = @Queue(name = "baz"),
				exchange = @Exchange(name = "baz.exchange"))
		)
		public String baz(String in) {
			return "baz:" + in;
		}
		
	}
}
