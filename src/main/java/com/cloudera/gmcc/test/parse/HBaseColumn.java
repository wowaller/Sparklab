package com.cloudera.gmcc.test.parse;

import org.apache.hadoop.hbase.util.Bytes;

public class HBaseColumn {
	public String family;
	public String column;
	public byte[] family_bytes;
	public byte[] column_bytes;

	public HBaseColumn(String family, String column) {
		this.family = family;
		this.column = column;
		this.family_bytes = Bytes.toBytes(this.family);
		this.column_bytes = Bytes.toBytes(this.column);
	}
}
