package com.broker.kafka.prac.utils;

import java.io.*;
import java.util.*;

public class PropertyFileReader {
	 Properties prop;

	public PropertyFileReader() throws FileNotFoundException, IOException{
		 prop = new Properties();
         File currentDirFile = new File(".");
 		 String helper = currentDirFile.getAbsolutePath().replace("\\.", "");
         String propertyFilePath = helper+"/config/Manage.properties";
         System.out.println("Loading property file:"+propertyFilePath);
         prop.load(new FileInputStream(propertyFilePath));
	}
	
	 public String getProperty(String key)
     {
        String keyValue = prop.getProperty(key);
        System.out.println(key+":"+keyValue);
        return keyValue;
     }
	     
}
