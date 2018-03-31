package cn.thousfeet.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsUtils {

	FileSystem fs = null;
	
	@Before
	public void init() throws Exception {
		
		// 读取classpath下的xxx-site.xml配置文件，解析内容封装到conf对象中
		Configuration conf = new Configuration();
		
		// 可以手动设置conf的配置信息（覆盖原配置文件中的值）
		conf.set("fs.defaultFS", "hdfs://node01:9000/");
		
		// 获取文件系统的客户端操作实例对象。设置username为之前上传文件的linux用户信息，以防没有读写权限
		fs = FileSystem.get(new URI("hdfs://node01:9000/"), conf, "thousfeet");
	}
	
	/**
	 * 上传文件
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void upload() throws IllegalArgumentException, Exception {
		
		fs.copyFromLocalFile(new Path("D:/eclipse-workspace/test.txt"), new Path("hdfs://node01:9000/a/b/c/test.txt"));
	}
	
	/**
	 * 下载文件
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void download() throws IllegalArgumentException, Exception {
		
		fs.copyToLocalFile(new Path("hdfs://node01:9000/aa/test.txt"), new Path("D:/eclipse-workspace/test2.txt"));
	}
	
	/**
	 * 查看文件信息
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 */
	@Test
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, Exception {
		
		// listFile() 列出的是文件信息，并且提供递归遍历（参数为true时 能递归查看文件夹内的文件）
		RemoteIterator<LocatedFileStatus> files =  fs.listFiles(new Path("/"), true);
		while(files.hasNext()) 
		{
			LocatedFileStatus file = files.next();
			Path filePath = file.getPath();
			String fileName = filePath.getName();
			System.out.println(fileName);
		}
		/**打印结果
		 *  test.txt
		 *	core-site.xml
		 *	hadoop-core-1.2.1.jar
		 *	jdk-8u161-linux-x64.tar.gz
		 */
		
		System.out.println("-----------------------");
			
		// listStatus() 列出文件和文件夹的信息，但是不提供递归遍历（需要自行去做递归）
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for(FileStatus status : listStatus) 
		{
			String name = status.getPath().getName();
			System.out.println(name + (status.isDirectory() ? " - is a dir": " - is a file"));
		}
		/**
		 * 打印结果
		 *  a - is a dir
		 *  core-site.xml - is a file
		 *  input - is a dir
		 *  jdk-8u161-linux-x64.tar.gz - is a file
		 */
	}
	
	/**
	 * 创建文件夹
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void mkdir() throws IllegalArgumentException, Exception {
		
		fs.mkdirs(new Path("/a/b/c"));
	}
	
	/**
	 * 删除文件或文件夹
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void deleteFiles() throws IllegalArgumentException, Exception {
		
		fs.delete(new Path("hdfs://node01:9000/aa"), true);
	}
}
