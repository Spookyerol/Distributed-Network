import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Client {
	
	public Client() {}
	
	public static void main(String[] args) {
		String host = (args.length < 1) ? null : args[0];
		try {

		    // Get registry
		    Registry registry = LocateRegistry.getRegistry("127.0.0.1", 3000);

		    // Lookup the remote object "Hello" from registry
		    // and create a stub for it
		    FEInterface stub = (FEInterface) registry.lookup("FE");

		    // Invoke a remote method
		    String response = stub.testRemote();
		    System.out.println("response: " + response);

		} catch (Exception e) {
			System.err.println("Client exception: " + e.toString());
			e.printStackTrace();
		}
	}
}
