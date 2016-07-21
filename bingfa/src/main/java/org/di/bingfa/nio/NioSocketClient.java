package org.di.bingfa.nio;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NioSocketClient {
	private final static Logger logger = Logger.getLogger(NioSocketClient.class.getName());

	public static void main(String[] args) throws Exception {
		for (int i = 0; i < 100; i++) {
			final int idx = i;
			new Thread(new MyRunnable(idx)).start();
		}
	}

	private static final class MyRunnable implements Runnable {

		private final int idx;

		private MyRunnable(int idx) {
			this.idx = idx;
		}

		public void run() {
			SocketChannel socketChannel = null;
			try {
				socketChannel = SocketChannel.open();
				SocketAddress socketAddress = new InetSocketAddress("localhost", 10000);
				socketChannel.connect(socketAddress);
				sendData(socketChannel, "hi");
				String res = receiveData(socketChannel);
				logger.log(Level.INFO, null, idx+" : "+res);
			} catch (Exception e) {
				logger.log(Level.SEVERE, null, idx+" : "+e);
			} finally {
				try {
					socketChannel.close();
				} catch (Exception ex) {
				}
			}
		}

		private void sendData(SocketChannel socketChannel, String data) throws IOException {
			byte[] bytes = data.getBytes();
			ByteBuffer buffer = ByteBuffer.wrap(bytes);
			socketChannel.write(buffer);
			socketChannel.socket().shutdownOutput();
		}

		private String receiveData(SocketChannel socketChannel) throws IOException {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			String rec;
			try {
				ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
				byte[] bytes;
				int count = 0;
				while ((count = socketChannel.read(buffer)) >= 0) {
					buffer.flip();
					bytes = new byte[count];
					buffer.get(bytes);
					baos.write(bytes);
					buffer.clear();
				}
				bytes = baos.toByteArray();
				rec = new String(bytes);
				socketChannel.socket().shutdownInput();
			} finally {
				try {
					baos.close();
				} catch (Exception ex) {
				}
			}
			return rec;
		}
	}
}
