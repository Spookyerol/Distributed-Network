import java.rmi.AccessException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.Random;

public class FEServer implements FEInterface {
	
	private static RInterface replicant; //Replicant we last made a connection with
	
	public FEServer() {}
	
	public static void main(String args[]) {
		
		try {
		    // Create server object
		    FEServer obj = new FEServer();

		    // Create remote object stub from server object
		    FEInterface stub = (FEInterface) UnicastRemoteObject.exportObject(obj, 0);

		    // Get registry
		    Registry registry = LocateRegistry.getRegistry("127.0.0.1", 3000);

		    // Bind the remote object's stub in the registry
		    registry.bind("FE", stub);
		    
		    Registry repRegistry = LocateRegistry.getRegistry("127.0.0.1", 10000);
		    replicant = (RInterface) repRegistry.lookup("R1");
		    
		    // Write ready message to console
		    System.err.println("Server ready");
		} catch (Exception e) {
		    System.err.println("Server exception: " + e.toString());
		    e.printStackTrace();
		}
	}
	
	public String connectToReplicant() {
		try {
			Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
			String[] replicants = registry.list();
			Random rand = new Random();
			int randInt = rand.nextInt(replicants.length);
			String binding = replicants[randInt];
			replicant = (RInterface) registry.lookup(binding);
			Status availability = replicant.getStatus();
			while(availability != Status.OK) {
				System.err.println("Replicant " + binding + " " + availability.name().toLowerCase());
				while(replicants[randInt] == binding) {
					randInt = rand.nextInt(replicants.length);
				}
				binding = replicants[randInt];
				replicant = (RInterface) registry.lookup(binding);
				availability = replicant.getStatus();
			}
			System.err.println("Replicant " + binding + " " + availability.name().toLowerCase());
			return replicants[randInt];
		} catch (Exception e) {
		    System.err.println("Server exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	public String getRating(int userID, int movieID) {
		try {
			String connection = connectToReplicant();
			System.out.println("Serving request from " + connection);
			return replicant.getRating(userID, movieID);
		} catch (Exception e) {
			System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	public String submitRating(int userID, int movieID, String score) {
		try {
			String connection = connectToReplicant();
			System.out.println("Serving request from " + connection);
			return replicant.submitRating(userID, movieID, score);
		} catch (Exception e) {
			System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
}

// rmiregistry <port number> on cmd before attempting to run.