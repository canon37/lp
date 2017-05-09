package com.zuipin.util;

public class StringUtil {
	public static boolean isNotBlank(String text) {
		if (text == null) {
			return false;
		}
		if (text.trim().length() <= 0) {
			return false;
		}
		if (text.toLowerCase().equals("null")) {
			return false;
		}
		return true;
	}
}
