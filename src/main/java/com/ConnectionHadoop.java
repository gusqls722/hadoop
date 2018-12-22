package com;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class ConnectionHadoop {
	public static void main(String args[]) {
		PrivilegedExceptionAction<Void> pea = new PrivilegedExceptionAction<Void>() {
			
			
			
			@Override
			public Void run() throws Exception {
				Configuration config = new Configuration();
				config.set("fs.defaultFS", "hdfs://192.168.0.162:9000/user/bdi");		// 앞에껀 키값 
				config.setBoolean("dfs.support.append", true);		

				FileSystem fs = FileSystem.get(config);		// 파일시스템에 겟해서 해당 콘피그 22번쨋 줄에 연결한거임.

				Path upFileName = new Path("word.txt");      // 워드라는 파일을
				if (fs.exists(upFileName)) {		// fs에 파일이 있다면
					fs.delete(upFileName, true);      // 지워버리고 나서
				}
				FSDataOutputStream fsdo = fs.create(upFileName);  // 만들고
				fsdo.writeUTF("hi hi hi hey hey lol start hi");        // 라는 내용으로 만들고
				fsdo.close();  // 책을 닫은것

				Path dirName = new Path("/user/bdi");         // user/bdi 안에 책을 찾아본다.
				FileStatus[] files = fs.listStatus(dirName);		// 찾아 넣는다.
				for (FileStatus file : files) {
					System.out.println(file);
				}
				return null;
			}
		};
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bdi");
		try {
			ugi.doAs(pea);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
