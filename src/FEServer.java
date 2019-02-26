import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.Random;

public class FEServer implements FEInterface {
	
	private int latestCounter; // The number of updates the last replica we looked at had
	private String replica; // Replica we last made a connection with
	
	
	public FEServer() {
		latestCounter = 0;
		replica = "FE"; //this means we haven't handled any request yet
	}
	
	public static void main(String args[]) {
		
		try {
		    FEServer obj = new FEServer();
		    // Create the remote interface with server obj
		    FEInterface stub = (FEInterface) UnicastRemoteObject.exportObject(obj, 0);
		    Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
		    registry.bind("FE", stub);
		    
		    System.err.println("Server ready at 10000");
		} catch (Exception e) {
		    System.err.println("Server exception: " + e.toString());
		    e.printStackTrace();
		}
	}
	
	// Connects to a remote interface and returns the remote object
	public RInterface connectToReplica() {
		try {
			// Get all the servers on the registry (including itself)
			Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
			String[] replicas = registry.list();
			// Randomly pick one of the replica servers
			Random rand = new Random();
			int randInt = rand.nextInt(replicas.length - 1) + 1; //pick in range (1-length) to avoid picking itslef
			String binding = replicas[randInt];
			RInterface replica = (RInterface) registry.lookup(binding);
			// Check if server available
			Status availability = replica.getStatus();
			while(availability != Status.OK) {
				System.err.println("Replica " + binding + " " + availability.name().toLowerCase());
				// Check the other replicas until one is Status.OK
				while(replicas[randInt] == binding) {
					randInt = rand.nextInt(replicas.length - 1) + 1;
				}
				binding = replicas[randInt];
				replica = (RInterface) registry.lookup(binding);
				availability = replica.getStatus();
			}
			System.err.println("replica " + binding + " " + availability.name().toLowerCase());
			System.out.println("Serving request from " + binding);
			return replica;
		} catch (Exception e) {
		    System.err.println("Server exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	// Function to handle data retrieval that accepts ints user and movie ids and returns a response message
	public String getRating(int userID, int movieID) {
		try {
			RInterface connection = connectToReplica();
			System.err.println("Last served request from " + replica);
			// Passes update information about the last visited replica to the current one to check for updates
			String[] lastVisited = {replica, Integer.toString(latestCounter)};
			String[] response = connection.getRating(userID, movieID, lastVisited);
			latestCounter = Integer.valueOf(response[1]);
			this.replica = response[2];
			return response[0];
		} catch (Exception e) {
			System.err.println("replica exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	// Function to handle data submission and updates that accepts ints user and movie ids, string score and returns a response message
	public String submitRating(int userID, int movieID, String score) {
		try {
			RInterface connection = connectToReplica();
			System.err.println("Last served request from " + replica);
			String[] response = connection.submitRating(userID, movieID, score);
			latestCounter = Integer.valueOf(response[1]);
			this.replica = response[2];
			return response[0];
		} catch (Exception e) {
			System.err.println("replica exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	// Unbind registry and stop execution when the client disconnects
	public void terminate(Registry registry) {
		try {
			registry.unbind("FE");
		} catch (Exception e) {
			System.err.println("replica exception: " + e.toString());
		    e.printStackTrace();
		}
		System.exit(0); // terminates execution of front end
	}
}

// rmiregistry <port number> on cmd before attempting to run.