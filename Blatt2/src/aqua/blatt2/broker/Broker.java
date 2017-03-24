package aqua.blatt2.broker;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.swing.JOptionPane;

import aqua.blatt2.common.msgtypes.RegisterRequest;
import aqua.blatt2.common.Properties;
import aqua.blatt2.common.msgtypes.DeregisterRequest;
import aqua.blatt2.common.msgtypes.HandoffRequest;
import aqua.blatt2.common.msgtypes.RegisterResponse;
import messaging.Endpoint;
import messaging.Message;

public class Broker {

	private static ClientCollection<InetSocketAddress> clients = new ClientCollection<>();
	private final static String FISH_NAME = "HTWG ";
	private static final int NUM_OF_THREADS = 3;
	private static Endpoint endPoint = new Endpoint(Properties.PORT);
	private static int counter = 0;
	private static boolean stopRequested = false;

	public Broker() {
	}

	public static class BrokerTask implements Runnable {

		private static Message message;
 
		public BrokerTask(Message message) {
			BrokerTask.message = message;

		}

		@Override
		public void run() {
 			playloadMessages(message);
		}

	}
	public static class Threads extends Thread{
		public Threads() {
 		}
		public void run(){
			System.out.println("bin da");
			int value = JOptionPane.showConfirmDialog(null, "Do you want to end the Thread ? " ,"End", JOptionPane.YES_NO_CANCEL_OPTION);
				if (value ==  JOptionPane.YES_OPTION) {
					System.out.println("wir gesetzt");
 					 stopRequested = true;
				}

		}
	}
	private static void playloadMessages(Message message) {

		if (message.getPayload() instanceof RegisterRequest)
			register(message.getSender());
		if (message.getPayload() instanceof DeregisterRequest)
			deregister((DeregisterRequest) message.getPayload());
		if (message.getPayload() instanceof HandoffRequest)
			handoffFish(message.getSender(), (HandoffRequest) message.getPayload());
	}

	public static void broker() throws InterruptedException {
		Threads threads = new Threads();
		threads.start();
		TimeUnit.SECONDS.sleep(1);
		ExecutorService executor = Executors.newFixedThreadPool(NUM_OF_THREADS);
		Runnable runner;
		
		
		System.out.println("hier bin ich " + stopRequested);
		if (!stopRequested) {
			while (true) {

				Message message = endPoint.blockingReceive();
				runner = new BrokerTask(message);
				executor.execute(runner);

			}
		}
		executor.shutdown();
 
	}

	 
	private static void register(InetSocketAddress broker) {

		String id = FISH_NAME + counter;
		clients.add(id, broker);
		counter++;

		endPoint.send(broker, new RegisterResponse(id));

	}

	private static void deregister(DeregisterRequest deregisterRequest) {

		int index = clients.indexOf(deregisterRequest.getId());
		if (index != -1)
			clients.remove(index);

	}

	private static void handoffFish(InetSocketAddress broker, HandoffRequest handoffRequest) {

		int index = clients.indexOf(broker);

		if (handoffRequest.getFish().getDirection().equals("LEFT"))
			broker = clients.getLeftNeighorOf(index);
		else
			broker = clients.getRightNeighorOf(index);

		endPoint.send(broker, handoffRequest);
	}

	public static void main(String[] args) throws InterruptedException {
		broker();

	}

}
