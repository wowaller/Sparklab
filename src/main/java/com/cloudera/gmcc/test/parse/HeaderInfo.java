package com.cloudera.gmcc.test.parse;

public class HeaderInfo {
	private String _name;
	private int _start;
	private int _length;

	public HeaderInfo(String name, int start, int length) {
		_name = name;
		_start = start;
		_length = length;
	}

	public String getName() {
		return _name;
	}

	public int getStart() {
		return _start;
	}

	public int getLength() {
		return _length;
	}
}
