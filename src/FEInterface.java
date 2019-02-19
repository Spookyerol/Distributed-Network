import java.rmi.Remote;
import java.rmi.RemoteException;

public interface FEInterface extends Remote {
	String getRating(int userID, int movieID) throws RemoteException;
}
