package dht.rush.utils;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.*;
import java.util.Date;


/**
 * This class provides services used to read & write data to
 * streams in various formats needed to implement the
 * project services.
 */
public class StreamUtil {
    public static JsonObject parseRequest(InputStream inputStream) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String str;
        JsonObject jsonObject = null;

        while ((str = br.readLine()) != null) {
        	System.out.println("Request received: " + str + " ---- " + new Date().toString());

            JsonReader jsonReader = Json.createReader(new StringReader(str));
            jsonObject = jsonReader.readObject();
            return jsonObject;
        }
        if (jsonObject == null) {
            throw new Exception("Empty Request!");
        }
        return jsonObject;
    }
    
    public static JsonObject parseRequest(BufferedReader br) throws Exception {
        String str;
        JsonObject jsonObject = null;

        while ((str = br.readLine()) != null) {
        	System.out.println("Request received: " + str + " ---- " + new Date().toString());

            JsonReader jsonReader = Json.createReader(new StringReader(str));
            jsonObject = jsonReader.readObject();
            return jsonObject;
        }
//        if (jsonObject == null) {
//            throw new Exception("Empty Request!");
//        }
        return jsonObject;
    }
    
    public static JsonObject parseRequest(String str) {
    	System.out.println("Request received: " + str + " ---- " + new Date().toString());

        JsonReader jsonReader = Json.createReader(new StringReader(str));
        JsonObject jsonObject = jsonReader.readObject();
        return jsonObject;
    }

    /**
     * Reads and returns the string without newline.
     * ByteArrayOutputStream is a very handy method of collecting
     * bytes when the length is unknown.
     */
    public static String readLine(InputStream inputStream) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for (int idx = 0; idx < 1000; idx++) {
            int ch = inputStream.read();
            if (ch == '\n' || ch == -1) {
                String result = baos.toString();
                return result;
            } else {
                baos.write(ch);
            }
        }
        System.err.println("************************");
        throw new IOException("No NewLine seen after 1000 characters");
    }

    /**
     * Reads and returns the next N bytes of data from the stream
     * in a byte array.
     */
    public static byte[] readData(int length, InputStream inputStream) throws IOException {
        byte bytes[] = new byte[length];
        for (int idx = 0; idx < length; idx++) {
            int ch = inputStream.read();
            bytes[idx] = (byte) ch;
        }
        return bytes;
    }

    public static void writeData(byte data[], OutputStream outputStream) throws IOException {
        outputStream.write(data);
        outputStream.flush();
    }

    public static void writeLine(String line, OutputStream outputStream) throws IOException {
        if (!line.endsWith("\n")) {
            line = line + "\n";
        }
        outputStream.write(line.getBytes());
        outputStream.flush();
    }

    /**
     * Close the socket by closing its input stream;
     */
    public static void closeSocket(InputStream inputStream) {
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException ex) {
            }
        }
    }

    public static void sendError(String errMsg, OutputStream outputStream) {
        if (!errMsg.endsWith("\n")) {
            errMsg = errMsg + "\n";
        }

        try {
            outputStream.write(errMsg.getBytes());
            outputStream.flush();
        } catch (IOException ex) {
        }
    }

}
