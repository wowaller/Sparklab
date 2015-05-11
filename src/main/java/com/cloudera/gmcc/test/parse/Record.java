package com.cloudera.gmcc.test.parse;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public abstract class Record {

    public static byte[][] genSplitKeys(String[] prefixes, int splitSize) {
        int round = 1;
        int num = 1;
        if (prefixes == null || prefixes.length == 0) {
            return null;
        } else if (splitSize == 1) {
            byte[][] splitKeys = new byte[prefixes.length][];
            for (int i = 0; i < prefixes.length; i++) {
                splitKeys[i] = prefixes[i].getBytes();
            }
            return splitKeys;
        } else {
            while ((round *= 10) < splitSize)
                num++;
            float step = (float) round / (float) splitSize;
            byte[][] splitKeys = new byte[splitSize * prefixes.length][];
            char[] digits = new char[num];
            String[] suffixes = new String[splitSize];

            for (int i = 0; i < splitSize; i++) {
                int digit = (int) (step * i);
                String strd = Integer.toString(digit);
                int index = 0;
                while (index < num - strd.length()) {
                    digits[index] = '0';
                    index++;
                }
                while (index < num) {
                    digits[index] = strd.charAt(index - num + strd.length());
                    index++;
                }
                String suffix = new String(digits);
                suffixes[i] = suffix;
            }
            // int index = 0;
            // while(index < num) { digits[index] = '9'; index ++; }
            // suffixes[splitSize-1] = new String(digits);

            Arrays.sort(prefixes);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < prefixes.length; i++)
                for (int j = 0; j < suffixes.length; j++) {
                    splitKeys[i * suffixes.length + j] = Bytes.toBytes(prefixes[i]
                            + suffixes[j]);
                    sb.append(prefixes[i] + suffixes[j] + "|");
                }
            System.out.println("Create regions: " + sb.toString());

            return splitKeys;
        }
    }

    public static Aggregate parseAggregates(String str) {
        if (str == null || str.length() == 0)
            return null;
        Aggregate aggr = new Aggregate();
        for (String s : str.split(",")) {
            if (s.startsWith("count")) {
                aggr.addCalculate(Aggregate.Operator.COUNT, "1");
            } else if (s.startsWith("sum")) {
                int i1 = s.indexOf("(");
                int i2 = s.indexOf(")");
                String obj = s.substring(i1 + 1, i2);
                aggr.addCalculate(Aggregate.Operator.SUM, obj);
            } else if (s.startsWith("double_sum")) {
                int i1 = s.indexOf("(");
                int i2 = s.indexOf(")");
                String obj = s.substring(i1 + 1, i2);
                aggr.addCalculate(Aggregate.Operator.DOUBLE_SUM, obj);
            } else if (s.startsWith("groupBy")) {
                int i1 = s.indexOf("(");
                int i2 = s.indexOf(")");
                String obj = s.substring(i1 + 1, i2);
                for (String key : obj.split(",")) {
                    aggr.addGroupByKey(key);
                }
            }
        }
        return aggr;
    }

    public abstract String toString();

    public abstract byte[][] getColumnFamilies();

    public abstract HBaseColumn[] getHBaseColumns();

    public abstract String getHBaseColumnValue(String family, String column);

    public abstract void setStoreEncoding(String encoding);

    public abstract byte[] getHBaseColumnValueInBytes(String family,
                                                      String column) throws IOException;

    public abstract String getHBaseRowKey();

    public abstract void skipFileHeader(BufferedReader reader, String fileName)
            throws IOException;

    public abstract boolean parse(BufferedReader reader, Record previous)
            throws IOException, ParseException; /*
                                                 * return true indicates
												 * there're more records, return
												 * false indicates end of stream
												 */

    public abstract void setTableNamePrefix(String prefix);

    ;

    public abstract String getTableNameFromFileName(String fileName)
            throws IOException;

    ;

    public boolean getWriteToWAL() {
        return true;
    }

    public void setWriteToWAL(boolean write) {
    }

    public abstract void constructSplitKeys(String[] prefixes, int splitSize);

    public abstract byte[][] getSplitKeys();

    public long getLongField(String field) {
        return 0;
    }

    public double getDoubleField(String field) {
        return 0.0;
    }

    public String getStringField(String field) {
        return "";
    }

    public static class Aggregate {
        private ArrayList<Calculate> calcs;
        private ArrayList<String> groupBy;

        public Aggregate() {
            calcs = new ArrayList<Calculate>();
            groupBy = new ArrayList<String>();
        }

        public void addCalculate(Operator op, String obj) {
            Calculate c = new Calculate();
            c.op = op;
            c.obj = obj;
            calcs.add(c);
        }

        public void addGroupByKey(String key) {
            groupBy.add(key);
        }

        public ArrayList<String> getGroupByKey() {
            return groupBy;
        }

        public ArrayList<Calculate> getCalculate() {
            return calcs;
        }

        public String genHBaseTableName() {
            StringBuilder sb = new StringBuilder();
            sb.append("AGGR");
            for (int i = 0; i < groupBy.size(); i++) {
                sb.append("_");
                sb.append(groupBy.get(i));
            }
            return sb.toString();
        }

        public ArrayList<String> genHBaseColumns() {
            ArrayList<String> objs = new ArrayList<String>();
            for (int i = 0; i < calcs.size(); i++) {
                objs.add(calcs.get(i).obj);
            }
            return objs;
        }

        public enum Operator {
            COUNT, SUM, DOUBLE_SUM
        }

        public static class Calculate {
            public String obj;
            public Operator op;
        }
    }
}
