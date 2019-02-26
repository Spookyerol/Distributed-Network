import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;

public interface FEInterface extends Remote {
	String getRating(int userID, int movieID) throws RemoteException;
	
	String submitRating(int userID, int movieID, String score) throws RemoteException;
	
	void terminate(Registry registry) throws RemoteException;
}
