import java.rmi.AccessException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.Random;

public class FEServer implements FEInterface {
	
	private int latestVector;
	private String replicant; //Replicant we last made a connection with
	
	
	public FEServer() {
		latestVector = 0;
		replicant = "FE"; 
	}
	
	public static void main(String args[]) {
		
		try {
		    // Create server object
		    FEServer obj = new FEServer();

		    // Create remote object stub from server object
		    FEInterface stub = (FEInterface) UnicastRemoteObject.exportObject(obj, 0);

		    // Get registry
		    Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);

		    // Bind the remote object's stub in the registry
		    registry.bind("FE", stub);
		    
		    // Write ready message to console
		    System.err.println("Server ready");
		} catch (Exception e) {
		    System.err.println("Server exception: " + e.toString());
		    e.printStackTrace();
		}
	}
	
	public RInterface connectToReplicant() {
		try {
			Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
			String[] replicants = registry.list();
			Random rand = new Random();
			int randInt = rand.nextInt(replicants.length - 1) + 1;
			String binding = replicants[randInt];
			//this.replicant = binding;
			RInterface replicant = (RInterface) registry.lookup(binding);
			Status availability = replicant.getStatus();
			while(availability != Status.OK) {
				System.err.println("Replicant " + binding + " " + availability.name().toLowerCase());
				while(replicants[randInt] == binding) {
					randInt = rand.nextInt(replicants.length - 1) + 1;
				}
				binding = replicants[randInt];
				//this.replicant = binding;
				System.err.println(binding);
				replicant = (RInterface) registry.lookup(binding);
				availability = replicant.getStatus();
			}
			System.err.println("Replicant " + binding + " " + availability.name().toLowerCase());
			System.out.println("Serving request from " + binding);
			return replicant;
		} catch (Exception e) {
		    System.err.println("Server exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	public String getRating(int userID, int movieID) {
		try {
			RInterface connection = connectToReplicant();
			System.err.println("Last served request from " + replicant);
			String[] vectorStamp = {replicant, Integer.toString(latestVector)};
			String[] response = connection.getRating(userID, movieID, vectorStamp);
			latestVector = Integer.valueOf(response[1]);
			this.replicant = response[2];
			return response[0];
		} catch (Exception e) {
			System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	public String submitRating(int userID, int movieID, String score) {
		try {
			RInterface connection = connectToReplicant();
			System.err.println("Last served request from " + replicant);
			String[] vectorStamp = {replicant, Integer.toString(latestVector)};
			String[] response = connection.submitRating(userID, movieID, score, vectorStamp);
			latestVector = Integer.valueOf(response[1]);
			this.replicant = response[2];
			return response[0];
		} catch (Exception e) {
			System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
}

// rmiregistry <port number> on cmd before attempting to run.