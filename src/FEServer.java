import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class FEServer implements FEInterface {
	
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

		    // Write ready message to console
		    System.err.println("Server ready");
		} catch (Exception e) {
		    System.err.println("Server exception: " + e.toString());
		    e.printStackTrace();
		}
	}
}

// rmiregistry <port number> on cmd before attempting to run.