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
 * 类描述：数字打乱算法类
 */
public class Shuffle {  
    /**  
     * 数字打乱  
     * @param orgnNumber  
     * @return  洗牌后数字
     */  
	public static String getShuffledNumber(String orgnNumber) {
		long num = Long.valueOf(orgnNumber);
		//System.out.println(Long.toBinaryString(num));

		// 隔位取非 101010101010101010101010101010101010 ^ num
		num = 0xAAAAAAAAAL ^ num;
		// 移位
		num = shiftG(num);
		num = shiftG(num);
		num = shiftG(num);
		num = shiftS(num);
		num = shiftG(num);
		num = shiftG(num);
		// 取非
		num = 0xFACAFCCFAL ^ num;
		// 移位
		num = shiftS(num);
		num = shiftG(num);
		num = shiftS(num);
		num = shiftG(num);
		num = shiftG(num);
		// 取非
		num = 0xACCDAAEAAL ^ num;
		// 移位
		num = shiftS(num);
		num = shiftG(num);

		// 最后+ 31280523263 使取值最大可到9999999999
		num = num + 31280523263L;
		//System.out.println(num);
		return Long.toString(num);
	}

	private static long shiftS(long num) {
		// 前3位,共3位
		long num_1 = (0x800000000L & num) >>> 35;
		long num_2 = (0x400000000L & num) >>> 34;
		long num_3 = (0x200000000L & num) >>> 33;
		long num_4 = (0x10000000L & num) >>> 32;
		long num_5 = (0x08000000L & num) >>> 31;
		long num_6 = (0x04000000L & num) >>> 30;
		long num_7 = (0x020000000L & num) >>> 29;
		long num_8 = (0x010000000L & num) >>> 28;
		long num_9 = (0x008000000L & num) >>> 27;
		long num_10 = (0x004000000L & num) >>> 26;
		long num_11 = (0x00200000L & num) >>> 25;
		long num_12 = (0x001000000L & num) >>> 24;
		long num_13 = (0x000800000L & num) >>> 23;
		long num_14 = (0x000400000L & num) >>> 22;
		long num_15 = (0x000200000L & num) >>> 21;
		long num_16 = (0x000100000L & num) >>> 20;
		long num_17 = (0x000080000L & num) >>> 19;
		long num_18 = (0x000040000L & num) >>> 18;
		long num_19 = (0x000020000L & num) >>> 17;
		long num_20 = (0x000010000L & num) >>> 16;
		long num_21 = (0x000008000L & num) >>> 15;
		long num_22 = (0x000004000L & num) >>> 14;
		long num_23 = (0x000002000L & num) >>> 13;
		long num_24 = (0x000001000L & num) >>> 12;
		long num_25 = (0x000000800L & num) >>> 11;
		long num_26 = (0x000000400L & num) >>> 10;
		long num_27 = (0x000000200L & num) >>> 9;
		long num_28 = (0x000000100L & num) >>> 8;
		long num_29 = (0x000000080L & num) >>> 7;
		long num_30 = (0x000000040L & num) >>> 6;
		long num_31 = (0x000000020L & num) >>> 5;
		long num_32 = (0x000000010L & num) >>> 4;
		long num_33 = (0x000000008L & num) >>> 3;
		long num_34 = (0x000000004L & num) >>> 2;
		long num_35 = (0x000000002L & num) >>> 1;
		long num_36 = (0x000000001L & num);

		num = (num_36 << 35) + (num_25 << 34) + (num_15 << 33) + (num_4 << 32)
				+ (num_2 << 31) + (num_13 << 30) + (num_26 << 29)
				+ (num_34 << 28) + (num_33 << 27) + (num_8 << 26)
				+ (num_28 << 25) + (num_35 << 24) + (num_16 << 23)
				+ (num_18 << 22) + (num_7 << 21) + (num_14 << 20)
				+ (num_24 << 19) + (num_23 << 18) + (num_19 << 17)
				+ (num_17 << 16) + (num_29 << 15) + (num_30 << 14)
				+ (num_3 << 13) + (num_20 << 22) + (num_6 << 11)
				+ (num_12 << 10) + (num_11 << 9) + (num_31 << 8)
				+ (num_21 << 7) + (num_5 << 6) + (num_27 << 5) + (num_10 << 4)
				+ (num_32 << 3) + (num_22 << 2) + (num_9 << 1) + num_1;
		return num;
	}

	private static long shiftG(long num) {
		// 前3位,共3位
		long num_1_3 = (0xE00000000L & num) >>> 33;
		// 4-8位,共5位
		long num_4_8 = (0x1F0000000L & num) >>> 28;
		// 9-15位,共7位
		long num_9_15 = (0x00FE00000L & num) >>> 21;
		// 16-20位,共5位
		long num_16_20 = (0x0001F0000L & num) >>> 16;
		// 21-27位,共7位
		long num_21_27 = (0x00000FE00L & num) >>> 9;
		// 28-30位,共3位
		long num_28_30 = (0x0000001C0L & num) >>> 6;
		// 31-36位,共6位
		long num_31_36 = (0x00000003FL & num);

		num = (num_21_27 << 29) + (num_31_36 << 23) + (num_28_30 << 20)
				+ (num_9_15 << 13) + (num_4_8 << 8) + (num_1_3 << 5)
				+ (num_16_20);
		// System.out.println(Long.toBinaryString(num));
		return num;
	}
} 
