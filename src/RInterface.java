import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RInterface extends Remote {
	String getRating(int userID, int movieID) throws RemoteException;
	
	String submitRating(int userID, int movieID, String score) throws RemoteException;
	
	Status getStatus() throws RemoteException;
}
