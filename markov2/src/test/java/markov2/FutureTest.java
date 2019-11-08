package markov2;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class FutureTest {

	public static void main(String[] args) {
		
		String string = null;
		System.out.println("point 1");
		CompletableFuture<String> future = getStringFuture(string);
		String str2="nothing";
		
		try {
			CompletableFuture<String> future2 = future.handleAsync((str, throwable) ->
			{
				if(str == null) return "null";
				else return str;
			});
			System.out.println("point 2");
			str2 = future2.get();
		} catch (InterruptedException | ExecutionException e1) {
			e1.printStackTrace();
		}
		
		System.out.println("doing things");
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("finished doing things");
		System.out.println(str2);
	}

	static CompletableFuture<String> getStringFuture(String string) {
		return CompletableFuture.supplyAsync(() -> 
		{
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return ""+string.length();
		});
	}
}
