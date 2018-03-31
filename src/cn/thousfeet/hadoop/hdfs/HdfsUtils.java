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
		
		// ��ȡclasspath�µ�xxx-site.xml�����ļ����������ݷ�װ��conf������
		Configuration conf = new Configuration();
		
		// �����ֶ�����conf��������Ϣ������ԭ�����ļ��е�ֵ��
		conf.set("fs.defaultFS", "hdfs://node01:9000/");
		
		// ��ȡ�ļ�ϵͳ�Ŀͻ��˲���ʵ����������usernameΪ֮ǰ�ϴ��ļ���linux�û���Ϣ���Է�û�ж�дȨ��
		fs = FileSystem.get(new URI("hdfs://node01:9000/"), conf, "thousfeet");
	}
	
	/**
	 * �ϴ��ļ�
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void upload() throws IllegalArgumentException, Exception {
		
		fs.copyFromLocalFile(new Path("D:/eclipse-workspace/test.txt"), new Path("hdfs://node01:9000/a/b/c/test.txt"));
	}
	
	/**
	 * �����ļ�
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void download() throws IllegalArgumentException, Exception {
		
		fs.copyToLocalFile(new Path("hdfs://node01:9000/aa/test.txt"), new Path("D:/eclipse-workspace/test2.txt"));
	}
	
	/**
	 * �鿴�ļ���Ϣ
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 */
	@Test
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, Exception {
		
		// listFile() �г������ļ���Ϣ�������ṩ�ݹ����������Ϊtrueʱ �ܵݹ�鿴�ļ����ڵ��ļ���
		RemoteIterator<LocatedFileStatus> files =  fs.listFiles(new Path("/"), true);
		while(files.hasNext()) 
		{
			LocatedFileStatus file = files.next();
			Path filePath = file.getPath();
			String fileName = filePath.getName();
			System.out.println(fileName);
		}
		/**��ӡ���
		 *  test.txt
		 *	core-site.xml
		 *	hadoop-core-1.2.1.jar
		 *	jdk-8u161-linux-x64.tar.gz
		 */
		
		System.out.println("-----------------------");
			
		// listStatus() �г��ļ����ļ��е���Ϣ�����ǲ��ṩ�ݹ��������Ҫ����ȥ���ݹ飩
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for(FileStatus status : listStatus) 
		{
			String name = status.getPath().getName();
			System.out.println(name + (status.isDirectory() ? " - is a dir": " - is a file"));
		}
		/**
		 * ��ӡ���
		 *  a - is a dir
		 *  core-site.xml - is a file
		 *  input - is a dir
		 *  jdk-8u161-linux-x64.tar.gz - is a file
		 */
	}
	
	/**
	 * �����ļ���
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void mkdir() throws IllegalArgumentException, Exception {
		
		fs.mkdirs(new Path("/a/b/c"));
	}
	
	/**
	 * ɾ���ļ����ļ���
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void deleteFiles() throws IllegalArgumentException, Exception {
		
		fs.delete(new Path("hdfs://node01:9000/aa"), true);
	}
}
