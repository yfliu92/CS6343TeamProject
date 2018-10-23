package dht.rush.commands;

import dht.rush.utils.StreamUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class ServerCommand {

    protected InputStream inputStream;
    protected OutputStream outputStream;

    abstract public void run() throws IOException;

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    protected void sendOK() throws IOException {
        String msg = "OK\n";
        outputStream.write(msg.getBytes());
        outputStream.flush();
    }

    protected void sendError(String errMsg) {
        StreamUtil.sendError(errMsg, outputStream);
    }

}
