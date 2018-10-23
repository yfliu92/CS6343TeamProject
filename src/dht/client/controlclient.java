//package dht.client;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.io.PrintWriter;
//import java.net.Socket;
//import java.util.Date;
//import java.util.Random;
//import java.lang.Thread;
//
//public class controlclient extends client {
//
//	static Random ran = new Random();
//	final static int maxlength = 15;
//	final static int minlength = 1;
//	public static String generateRanStr() {
//		StringBuilder word = new StringBuilder();
//		int length = minlength + ran.nextInt(maxlength);
//		while(word.length() < length) {
//			word.append((char)(ran.nextInt(26) + 97));
//		}
//		return word.toString();
//	}
//
//	public static void main(String[] args) throws IOException {
//		// TODO Auto-generated method stub
//
//		String serverAddress = "localhost";
//        Socket s = new Socket(serverAddress, 9090);
//        PrintWriter out =
//                new PrintWriter(s.getOutputStream(), true);
//        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
//        while(true) {
//            out.println("add " + generateRanStr() + " | X");
////	        BufferedReader input =
////	            new BufferedReader(new InputStreamReader(s.getInputStream()));
//	        String answer = input.readLine();
//	//        JOptionPane.showMessageDialog(null, answer);
//	        String timeReceived = new Date().toString();
//	        System.out.println(timeReceived + " -- response Received: " + answer);
//
//	        try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//        }
//
////        System.exit(0);
//	}
//
//}
