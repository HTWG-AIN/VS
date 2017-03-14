package aqua.blatt1.broker;

import java.net.InetSocketAddress;

import aqua.blatt1.common.Properties;
import aqua.blatt1.common.msgtypes.DeregisterRequest;
import aqua.blatt1.common.msgtypes.HandoffRequest;
import aqua.blatt1.common.msgtypes.RegisterRequest;
import aqua.blatt1.common.msgtypes.RegisterResponse;
import messaging.Endpoint;
import messaging.Message;

public class Broker {

	private static ClientCollection<InetSocketAddress> clients = new ClientCollection<>();
	private final static String FISH_NAME = "HTWG ";
	private static Endpoint endPoint = new Endpoint(Properties.PORT);

	public Broker() {
	}

	public static void broker() {
 		while (true) {
 		 	Message message = endPoint.blockingReceive();
	 
		 	if(message.getPayload() instanceof RegisterRequest) 
		 		register(message.getSender() );
		 	if(message.getPayload() instanceof DeregisterRequest) 
		 		deregister((DeregisterRequest) message.getPayload());
		 	if(message.getPayload() instanceof HandoffRequest) 
		 		handoffFish((RegisterResponse) message.getPayload(),  message.getSender(), (HandoffRequest) message.getPayload()); 
				
			
		 	 
		  
		 	
		 	
		}
	}

	private static void register(InetSocketAddress broker ) {
		int counter = 0;
		String id = FISH_NAME + counter;
		clients.add(id, broker);
		counter++;

		endPoint.send(broker, new RegisterResponse(id));

	}

	private static void deregister( DeregisterRequest deregisterRequest) {
		
		int index = clients.indexOf(deregisterRequest.getId());
		if (index != -1)
			clients.remove(index);

	}

	private static void handoffFish(RegisterResponse registerResponse, InetSocketAddress broker, HandoffRequest handoffRequest) {
	
		int index = clients.indexOf(registerResponse.getId());

		if (handoffRequest.getFish().getDirection().equals("LEFT"))
			broker = clients.getLeftNeighorOf(index);
		else
			broker = clients.getRightNeighorOf(index);

		endPoint.send(broker, handoffRequest);
	}

	public static void main(String[] args) {
 		broker();
	}

}
