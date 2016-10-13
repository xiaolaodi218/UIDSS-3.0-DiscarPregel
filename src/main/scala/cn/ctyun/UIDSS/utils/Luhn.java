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
 * 类描述：Luhn算法类
 */
public class Luhn {  
    /**  
     * Luhn算法  
     * 根据卡号获取校验位  
     * @param cardNumber  
     * @return  
     */  
    public static int getCheckNumber(String cardNumber){  
        int totalNumber = 0;  
        for (int i = cardNumber.length()-1; i >= 0; i-=2) {  
            int tmpNumber = calculate(Integer.parseInt(String.valueOf(cardNumber.charAt(i))) *  2);  
            if (i==0) {  
                totalNumber += tmpNumber;  
            }else {  
                totalNumber += tmpNumber + Integer.parseInt(String.valueOf(cardNumber.charAt(i-1)));  
            }  
              
        }  
        if (totalNumber >= 0 && totalNumber < 9) {  
            return (10 - totalNumber);  
        }else {  
            String str = String.valueOf(totalNumber);  
            if (Integer.parseInt(String.valueOf(str.charAt(str.length()-1))) == 0) {  
                return 0;   
            }else {  
                return (10 - Integer.parseInt(String.valueOf(str.charAt(str.length()-1))));  
            }  
        }  
          
    }  
      
    /**  
     * 计算数字各位和  
     * @param number  
     * @return  
     */  
    public static int calculate(int number){  
        String str = String.valueOf(number);  
        int total = 0;  
        for (int i = 0; i < str.length(); i++) {  
            total += Integer.valueOf(Integer.parseInt(String.valueOf(str.charAt(i))));  
        }  
        return total;  
    }  

} 
