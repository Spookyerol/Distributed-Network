import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class FEServer implements FEInterface {
	
	private static RInterface replicant;
	
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
	
	public String getRating(int userID, int movieID) {
		try {
			return replicant.getRating(userID, movieID);
		} catch (Exception e) {
			System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
}

// rmiregistry <port number> on cmd before attempting to run.