package cn.ctyun.UIDSS.utils;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

public class KerberorsJavaUtil {
	private static final Logger LOG = Logger.getLogger(KerberorsJavaUtil.class);
	
	public static void getHBaseAuthentication(Configuration hconf,Properties props,String keytabFile){
		//get the keytab file path which added from spark submit "--files"
		String keyFilePath = KerberorsJavaUtil.class.getResource("/").getPath();
		LOG.info("=====file path====="+keyFilePath);
	    if(keyFilePath.startsWith("file")){
	    	keyFilePath = keyFilePath.substring(5);
	    }
	    //method "loginUserFromKeytab" required keyFilePath like "AAA/XXX/./keyFile"
	    keyFilePath = keyFilePath+"./"+keytabFile;
		LOG.info("------Start Get HBaseAuthentication-----");
		System.setProperty("java.security.krb5.conf",props.getProperty("krb5ConfDir"));
		hconf.set("hbase.security.authentication","kerberos");  
		hconf.set("hadoop.security.authentication","Kerberos");
		//hdfs-site.xml中namenode principal配置信息
		hconf.set("hbase.master.kerberos.principal",props.getProperty("masterPrin")); 
		//hdfs-site.xml中datanode principal配置信息
		hconf.set("hbase.regionserver.kerberos.principal",props.getProperty("regionPrin"));  
		UserGroupInformation.setConfiguration(hconf);  
	    try {
	    	//kerberos 认证 ,指定认证用户及keytab文件路径。
	    	LOG.info("------dev_yx.keytab path is---"+keyFilePath);
	    	UserGroupInformation.loginUserFromKeytab(props.getProperty("userName"),keyFilePath);
	    	LOG.info("------Get HBaseAuthentication Successed-----");
	    } catch (Exception e) {  
	        LOG.error("Get HBaseAuthentication Failed",e);  
	    }
	   
	}
	
	public static User getAuthenticatedUser(Configuration hconf,Properties props,String keytabFile){
		getHBaseAuthentication(hconf,props,keytabFile);		
		User loginedUser = null;
	    try {
	    	LOG.info("=====put the logined userinfomation to user====");
			loginedUser = User.create(UserGroupInformation.getLoginUser());
		} catch (IOException e) {
			LOG.error("===fialed put the logined userinfomation to user===",e);
		}	    
	    return loginedUser;
	}
	
}
