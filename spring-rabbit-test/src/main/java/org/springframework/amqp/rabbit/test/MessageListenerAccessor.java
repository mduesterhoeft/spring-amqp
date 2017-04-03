package org.springframework.amqp.rabbit.test;

import java.util.*;
import java.util.stream.Collectors;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.AbstractAdaptableMessageListener;

class MessageListenerAccessor {

	private final RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry;
    private final Collection<AbstractMessageListenerContainer> simpleMessageListenerContainers;
	private final Collection<Binding> bindings;

	MessageListenerAccessor(RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry,
							Collection<AbstractMessageListenerContainer> simpleMessageListenerContainers,
							Collection<Binding> bindings) {
		this.rabbitListenerEndpointRegistry = rabbitListenerEndpointRegistry;
		this.simpleMessageListenerContainers = simpleMessageListenerContainers;
		this.bindings = bindings;
	}

	List<AbstractAdaptableMessageListener> getListenerContainersForDestination(String exchange) {
		List<AbstractMessageListenerContainer> listenerContainers = collectListenerContainers();
		Set<String> queueNames = collectQueuesBoundToDestination(exchange);
		List<AbstractMessageListenerContainer> listenersByBoundQueues = getListenersByBoundQueues(listenerContainers, queueNames);
		return listenersByBoundQueues.stream()
				.map(s -> (AbstractAdaptableMessageListener) s.getMessageListener())
				.collect(Collectors.toList());
	}

	private List<AbstractMessageListenerContainer> getListenersByBoundQueues(List<AbstractMessageListenerContainer> listenerContainers, Set<String> queueNames) {
		List<AbstractMessageListenerContainer> matchingContainers = new ArrayList<>();
		for (AbstractMessageListenerContainer listenerContainer : listenerContainers) {
			if (listenerContainer.getQueueNames() != null) {
				for (String queueName :  listenerContainer.getQueueNames()) {
					if (queueNames.contains(queueName)) {
						matchingContainers.add(listenerContainer);
						break;
					}
				}
			}
		}
		return matchingContainers;
	}

	private Set<String> collectQueuesBoundToDestination(String destination) {
		Set<String> queueNames = new HashSet<>();
		for (Binding binding: this.bindings) {
			if (destination.equals(binding.getExchange()) && Binding.DestinationType.QUEUE.equals(binding.getDestinationType())) {
				queueNames.add(binding.getDestination());
			}
		}
		return queueNames;
	}

	private List<AbstractMessageListenerContainer> collectListenerContainers() {
		List<AbstractMessageListenerContainer> listenerContainers = new ArrayList<>();
		if (this.simpleMessageListenerContainers != null) {
			listenerContainers.addAll(this.simpleMessageListenerContainers);
		}
		if (this.rabbitListenerEndpointRegistry != null) {
			for (MessageListenerContainer listenerContainer : this.rabbitListenerEndpointRegistry.getListenerContainers()) {
				if (listenerContainer instanceof SimpleMessageListenerContainer) {
					listenerContainers.add((SimpleMessageListenerContainer) listenerContainer);
				}
			}
		}
		return listenerContainers;
	}
}