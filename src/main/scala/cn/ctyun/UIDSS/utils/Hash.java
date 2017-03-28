/*********************************************************************
 * 
 * CHINA TELECOM CORPORATION CONFIDENTIAL
 * ______________________________________________________________
 * 
 *  [2015] - [2020] China Telecom Corporation Limited, 
 *  All Rights Reserved.
 * 
 * NOTICE:  All information contained herein is, and remains
 * the property of China Telecom Corporation and its suppliers,
 * if any. The intellectual and technical concepts contained 
 * herein are proprietary to China Telecom Corporation and its 
 * suppliers and may be covered by China and Foreign Patents,
 * patents in process, and are protected by trade secret  or 
 * copyright law. Dissemination of this information or 
 * reproduction of this material is strictly forbidden unless prior 
 * written permission is obtained from China Telecom Corporation.
 **********************************************************************/
package cn.ctyun.UIDSS.utils;

/**
 * 类描述：Hash算法类
 */
public class Hash {  
	
    /**  
     * Hash算法  
     * 根据ASCII码数字和获取哈西值  
     * @param strInput 长度大于0的字符串 
     * @return  取值为"0-9"或 "@-Z"或 "a-z"或“.”的两个字符。共64x64共4096种可能
     */  
    public static String getHashString(String strInput){  
    	//计算数字各位和  
        int total = 0;  
        for (int i = 0; i < strInput.length(); i++) {  
            total += strInput.charAt(i);  
        }
        
        char chrF = '0' ;
        char chrS = '0' ; 
        	
        int fLetter = (total+17)*97%64;
        int sLetter = (total+31)%64;

        if (fLetter<10) {
            chrF = (char) (fLetter + '0') ;
        } else if (fLetter<37) {
        	chrF = (char) (fLetter - 10 + '@') ;
        } else if (fLetter<63){
        	chrF = (char) (fLetter -37 + 'a') ;
        } else {
        	chrF = '.';
        }
        
        if (sLetter<10) {
            chrS = (char) (sLetter + '0') ;
        } else if (sLetter<37) {
        	chrS = (char) (sLetter - 10 + '@') ;
        } else if (sLetter < 63){
        	chrS = (char) (sLetter -37 + 'a') ;
        } else {
        	chrS = '.';
        }
        //System.out.println(String.valueOf(chrF) +  String.valueOf(chrS) ) ;  
        return String.valueOf(chrF) +  String.valueOf(chrS)  ;          
    }  
} 
