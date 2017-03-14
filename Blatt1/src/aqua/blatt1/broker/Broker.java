package aqua.blatt1.broker;

import java.net.InetSocketAddress;

import aqua.blatt1.client.Aqualife;
import aqua.blatt1.common.Properties;
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
			String[] args = {};
		//	Message message = endPoint.blockingReceive();
			Aqualife.main(args);
			//	register(message.getSender(), (String) message.getPayload());

		}
	}

	private static void register(InetSocketAddress broker, String id) {
		int counter = 0;
		id = FISH_NAME + counter;
		clients.add(id, broker);
		counter++;

		endPoint.send(broker, new RegisterResponse(id));

	}

	private static void deregister(String id) {
		int index = clients.indexOf(id);
		if (index != -1)
			clients.remove(index);

	}

	private static void handoffFish(String id, InetSocketAddress broker, HandoffRequest handoffRequest) {
		int index = clients.indexOf(id);

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
