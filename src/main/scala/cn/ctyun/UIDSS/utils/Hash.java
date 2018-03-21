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

import org.apache.hadoop.hbase.util.Bytes;

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
//		int total = 0;
//		// for (int i = 0; i < strInput.length(); i++) {
//		// total += strInput.charAt(i)*(i%7+1);
//		// }
//
//		int hash, i;
//		for (hash = strInput.length(), i = 0; i < strInput.length(); ++i)
//			hash = (hash << 4) ^ (hash >> 28) ^ strInput.charAt(i);
//		total = hash % 3719;
//
//		char chrF = '0';
//		char chrS = '0';
//
//		int fLetter = total / 61;
//		// int s = (total/64);
//		// int sLetter = (s%2)*32 + ((s>>1)%2)*16 + ((s>>2)%2)*8 + ((s>>3)%2)*4
//		// + ((s>>4)%2)*2+ ((s>>5)%2);
//		int sLetter = total % 61;
//
//		if (fLetter < 9) {
//			chrF = (char) (fLetter + '1');
//		}   else if (fLetter<35) {
//        	chrF = (char) (fLetter - 9 + 'A') ;
//        }   else {
//			chrF = (char) (fLetter - 35 + 'a');
//		}
//
//		if (sLetter < 9) {
//			chrS = (char) (sLetter + '1');
//		}   else if (sLetter<35) {
//			chrS = (char) (sLetter - 9 + 'A') ;
//        }   else {
//        	chrS = (char) (sLetter - 35 + 'a');
//		}
//		int hash, i;
//		for (hash = strInput.length(), i = 0; i < strInput.length(); ++i)
//			hash = (hash << 5) ^ (hash >> 27) ^ strInput.charAt(i);
//
//		if (hash <0 ) {	hash = -hash; 	}
//		
//		hash = hash % 3719;
//
//		byte[] sHead = {0,0};
//
//		int fLetter = hash / 61;
//		int sLetter = hash % 61;
//
//		if (fLetter < 9) {
//			sHead[0] = (byte) (fLetter + '1');
//		}   else if (fLetter<35) {
//			sHead[0] = (byte) (fLetter - 9 + 'A') ;
//        }   else {
//        	sHead[0] = (byte) (fLetter - 35 + 'a');
//		}
//
//		if (sLetter < 9) {
//			sHead[1] = (byte) (sLetter + '1');
//		}   else if (sLetter<35) {
//			sHead[1] = (byte) (sLetter - 9 + 'A') ;
//        }   else {
//        	sHead[1] = (byte) (sLetter - 35 + 'a');
//		}
//		
//		return Bytes.toString(sHead);
        //System.out.println(String.valueOf(chrF) +  String.valueOf(chrS) ) ;  
        //return String.valueOf(chrF) +  String.valueOf(chrS)  ;  
    	
		int hash, i;
		for (hash = strInput.length(), i = 0; i < strInput.length(); ++i)
			hash = (hash << 5) ^ (hash >> 27) ^ strInput.charAt(i);

		if (hash < 0) {	hash = -hash; 	}
		hash = hash % 4093;

		char[] sHead = { 0, 0 };
		int fLetter = hash / 64;
		int sLetter = hash % 64;

		if (fLetter < 10) {
			sHead[0] = (char) (fLetter + '0');
		} else if (fLetter < 37) {
			sHead[0] = (char) (fLetter - 10 + '@');
		} else if (fLetter < 63) {
			sHead[0] = (char) (fLetter - 37 + 'a');
		} else {
			sHead[0] = '~';
		}

		if (sLetter < 10) {
			sHead[1] = (char) (sLetter + '0');
		} else if (sLetter < 37) {
			sHead[1] = (char) (sLetter - 10 + '@');
		} else if (sLetter < 63) {
			sHead[1] = (char) (sLetter - 37 + 'a');
		} else {
			sHead[1] = '~';
		}

		return String.valueOf(sHead);
    }  
} 
