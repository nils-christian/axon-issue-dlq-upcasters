package com.example.demo;

import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.config.ConfigurerModule;
import org.axonframework.config.EventProcessingConfiguration;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.PropagatingErrorHandler;
import org.axonframework.eventhandling.deadletter.jpa.JpaDeadLetter;
import org.axonframework.eventhandling.deadletter.jpa.JpaSequencedDeadLetterQueue;
import org.axonframework.eventhandling.gateway.EventGateway;
import org.axonframework.messaging.deadletter.SequencedDeadLetterQueue;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.IntermediateEventRepresentation;
import org.axonframework.serialization.upcasting.event.SingleEventUpcaster;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

@SpringBootApplication
public class DeadLetterQueueDemo {

	public static void main( final String[] args ) {
		final ConfigurableApplicationContext applicationContext = SpringApplication.run( DeadLetterQueueDemo.class, args );

		final EventGateway eventGateway = applicationContext.getBean( EventGateway.class );
		final MyEvent originalEvent = new MyEvent( "0" );
		eventGateway.publish( originalEvent );

		final EventProcessingConfiguration eventProcessingConfiguration = applicationContext.getBean( EventProcessingConfiguration.class );
		final SequencedDeadLetterQueue<EventMessage<?>> deadLetterQueue = eventProcessingConfiguration.deadLetterQueue( "P1" ).orElseThrow( );
		await( )
				.atMost( Duration.ofSeconds( 30 ) )
				.until( ( ) -> deadLetterQueue.size( ) == 1 );

		final JpaDeadLetter<EventMessage<MyEvent>> deadLetter = ( JpaDeadLetter<EventMessage<MyEvent>> ) deadLetterQueue.deadLetters( ).iterator( ).next( ).iterator( ).next( );
		final MyEvent eventStoredInDLQ = deadLetter.message( ).getPayload( );
		System.out.println( "Original event: " + originalEvent );
		System.out.println( "Event stored in DLQ: " + eventStoredInDLQ );
	}

	@Bean
	ConfigurerModule eventProcessingConfigurerModule( final EntityManagerProvider entityManagerProvider, final Serializer serializer ) {
		return c -> c
				.eventProcessing( )
				.registerDefaultListenerInvocationErrorHandler( conf -> PropagatingErrorHandler.INSTANCE )
				.registerDeadLetterQueue( "P1", conf -> {
					final EventProcessingConfiguration eventProcessingConfiguration = conf.eventProcessingConfiguration( );
					return JpaSequencedDeadLetterQueue
							.builder( )
							.processingGroup( "P1" )
							.transactionManager( eventProcessingConfiguration.transactionManager( "P1" ) )
							.entityManagerProvider( entityManagerProvider )
							.serializer( serializer )
							.build( );
				} );
	}

	@Component
	@ProcessingGroup( "P1" )
	public class MyEventHandler {

		@EventHandler
		public void on( final MyEvent e ) {
			throw new IllegalStateException( "I cannot handle this event: " + e );
		}

	}

	@Component
	public class MyUpcaster extends SingleEventUpcaster {

		@Override
		protected boolean canUpcast( final IntermediateEventRepresentation intermediateRepresentation ) {
			return true;
		}

		@Override
		protected IntermediateEventRepresentation doUpcast( final IntermediateEventRepresentation intermediateRepresentation ) {
			return intermediateRepresentation.upcastPayload( intermediateRepresentation.getType( ), ObjectNode.class, o -> {
				final ObjectNode newPayload = JsonNodeFactory.instance.objectNode( );
				final String newId = "MyNewPrefix-" + o.get( "id" ).textValue( );
				newPayload.put( "id", newId );
				return newPayload;
			} );
		}

	}

	public static class MyEvent {

		private String id;

		public MyEvent( ) {
		}

		public MyEvent( final String id ) {
			this.id = id;
		}

		public String getId( ) {
			return id;
		}

		public void setId( final String id ) {
			this.id = id;
		}

		@Override
		public String toString( ) {
			return "MyEvent[id=" + id + "]";
		}

	}

}