package com.cloudera.gmcc.test.parse;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class BOSSRecordBack extends Record {

    public static final int PHONE_NUM_LENGTH = 24;
    public static final int START_TIME_LENGTH = 14;
    public static final String DELIMITER = "|";
    public static final byte[] FAMILY_NAME = Bytes.toBytes("Info");
    // public static final byte[] COLUMN_EVENTFORMATTYPE =
    // Bytes.toBytes("EventFormatType");
    // public static final byte[] COLUMN_BASIC = Bytes.toBytes("Basic");
    // public static final byte[] COLUMN_EXTENDED = Bytes.toBytes("Extended");
    public static final HBaseColumn COLUMN_COMMON = new HBaseColumn("Info",
            "Common");
    public static final HBaseColumn COLUMN_EVENTFORMATTYPE = new HBaseColumn(
            "Info", "EventFormatType");
    public static final HBaseColumn COLUMN_AREA = new HBaseColumn("Info",
            "Area");
    public static final HBaseColumn COLUMN_BASIC = new HBaseColumn("Info",
            "CBE_Basic");
    public static final HBaseColumn COLUMN_EXTENDED_VOICE = new HBaseColumn(
            "Info", "CBE_voice");
    public static final HBaseColumn COLUMN_EXTENDED_SMS = new HBaseColumn(
            "Info", "CBE_sms");
    public static final HBaseColumn COLUMN_EXTENDED_GPRS = new HBaseColumn(
            "Info", "CBE_gprs");
    public static final HBaseColumn COLUMN_EXTENDED_SPECIAL = new HBaseColumn(
            "Info", "CBE_special");
    public static final HBaseColumn COLUMN_EXTENDED_WLAN = new HBaseColumn(
            "Info", "CBE_wlan");
    public static final HBaseColumn COLUMN_EXTENDED_FIXED_BILL = new HBaseColumn(
            "Info", "Fixed_Bill");
    public static final HBaseColumn COLUMN_EXTENDED_OTHER_BILL = new HBaseColumn(
            "Info", "Other_Bill");
    public static final HBaseColumn[][] HBASE_COLUMNS = {
            {}, // 0 dummy
            {COLUMN_EVENTFORMATTYPE, COLUMN_AREA, COLUMN_BASIC,
                    COLUMN_EXTENDED_VOICE, COLUMN_COMMON},
            {COLUMN_EVENTFORMATTYPE, COLUMN_AREA, COLUMN_BASIC,
                    COLUMN_EXTENDED_SMS, COLUMN_COMMON},
            {COLUMN_EVENTFORMATTYPE, COLUMN_AREA, COLUMN_BASIC,
                    COLUMN_EXTENDED_GPRS, COLUMN_COMMON},
            {COLUMN_EVENTFORMATTYPE, COLUMN_AREA, COLUMN_BASIC,
                    COLUMN_EXTENDED_SPECIAL, COLUMN_COMMON},
            {COLUMN_EVENTFORMATTYPE, COLUMN_AREA, COLUMN_BASIC,
                    COLUMN_EXTENDED_WLAN, COLUMN_COMMON},
            {COLUMN_EVENTFORMATTYPE, COLUMN_AREA, COLUMN_BASIC,
                    COLUMN_EXTENDED_FIXED_BILL, COLUMN_COMMON},
            {COLUMN_EVENTFORMATTYPE, COLUMN_AREA, COLUMN_BASIC,
                    COLUMN_EXTENDED_OTHER_BILL, COLUMN_COMMON},
            {}, // 8 dummy
            {}, // 9 dummy
            {}, // 10 dummy
            {COLUMN_EVENTFORMATTYPE, COLUMN_AREA, COLUMN_BASIC,
                    COLUMN_EXTENDED_VOICE, COLUMN_COMMON},
            {COLUMN_EVENTFORMATTYPE, COLUMN_AREA, COLUMN_BASIC,
                    COLUMN_EXTENDED_SMS, COLUMN_COMMON},
            {COLUMN_EVENTFORMATTYPE, COLUMN_AREA, COLUMN_BASIC,
                    COLUMN_EXTENDED_GPRS, COLUMN_COMMON},
            {}, // 14 dummy
            {COLUMN_EVENTFORMATTYPE, COLUMN_AREA, COLUMN_BASIC,
                    COLUMN_EXTENDED_WLAN, COLUMN_COMMON},};
    public static final HeaderInfo[] HEADER_COMMON_CBE = {
            new HeaderInfo("EventFormatType", 0, 2),
            new HeaderInfo("roll_flag", 2, 1),
            new HeaderInfo("roll_count", 3, 2),
            new HeaderInfo("file_id", 5, 10), new HeaderInfo("exc_id", 15, 4),
            new HeaderInfo("proc_time", 91, 14),
            new HeaderInfo("Switch_flag", 125, 2),
            new HeaderInfo("Brand", 129, 1),
            new HeaderInfo("Bill_type", 164, 2),
            new HeaderInfo("xd_type", 649, 1)};
    public static final HeaderInfo[] HEADER_COMMON_CBE_GROUP = {
            new HeaderInfo("EventFormatType", 0, 2),
            new HeaderInfo("roll_flag", 2, 1),
            new HeaderInfo("roll_count", 3, 2),
            new HeaderInfo("file_id", 5, 10), new HeaderInfo("exc_id", 15, 4),
            new HeaderInfo("proc_time", 115, 14), // Change here
            new HeaderInfo("Switch_flag", 149, 2), // Change here
            new HeaderInfo("Brand", 153, 1), // Change here
            new HeaderInfo("Bill_type", 188, 2), // Change here
            new HeaderInfo("xd_type", 673, 1) // Change here
    };
    public static final HeaderInfo[] HEADER_COMMON_BILL = {
            new HeaderInfo("EventFormatType", 0, 2),
            new HeaderInfo("roll_flag", 2, 1),
            new HeaderInfo("roll_count", 3, 2),
            new HeaderInfo("file_id", 5, 10), new HeaderInfo("exc_id", 15, 4),
            new HeaderInfo("proc_time", 43, 14),
            new HeaderInfo("Switch_flag", 57, 2),
            new HeaderInfo("Brand", 59, 1), new HeaderInfo("Bill_type", 60, 2),
            new HeaderInfo("acct_balance_b", 88, 12),
            new HeaderInfo("xd_type", 130, 1)};
    public static final HeaderInfo[][] HEADER_BASIC_APPENDIX = {
            null,
            {new HeaderInfo("dis_fee", 723, 8),
                    new HeaderInfo("duration", 763, 7),
                    new HeaderInfo("real_msisdn", 824, 24),},
            {new HeaderInfo("dis_fee", 722, 8),
                    new HeaderInfo("duration", 876, 7),
                    new HeaderInfo("real_msisdn", 786, 24),},
            {new HeaderInfo("dis_fee", 719, 8),
                    new HeaderInfo("duration", 934, 7),
                    new HeaderInfo("real_msisdn", 782, 24),},
            {new HeaderInfo("dis_fee", 723, 8),
                    new HeaderInfo("duration", 761, 7),
                    new HeaderInfo("real_msisdn", 840, 24),},
            {new HeaderInfo("dis_fee", 714, 8),
                    new HeaderInfo("duration", 789, 7),
                    new HeaderInfo("real_msisdn", 765, 24),}};
    public static final HeaderInfo[] HEADER_BASIC = {
            new HeaderInfo("EventFormatType", 0, 2),
            new HeaderInfo("roll_flag", 2, 1),
            new HeaderInfo("roll_count", 3, 2),
            new HeaderInfo("file_id", 5, 10), new HeaderInfo("exc_id", 15, 4),
            new HeaderInfo("FileType", 19, 2), new HeaderInfo("subno", 21, 24),
            new HeaderInfo("IMSI", 45, 15), new HeaderInfo("IMEI", 60, 15),
            new HeaderInfo("start_time", 75, 14),
            new HeaderInfo("special_flag", 89, 2),
            new HeaderInfo("proc_time", 91, 14),
            new HeaderInfo("event_id", 105, 20),
            new HeaderInfo("Switch_flag", 125, 2),
            new HeaderInfo("District", 127, 2),
            new HeaderInfo("Brand", 129, 1),
            new HeaderInfo("User_Type", 130, 2),
            new HeaderInfo("Visit_Area", 132, 8),
            new HeaderInfo("B_subno", 140, 24),
            new HeaderInfo("Bill_type", 164, 2),
            new HeaderInfo("ACCT_Mob", 166, 14),
            new HeaderInfo("ACCT_Toll", 180, 14),
            new HeaderInfo("ACCT_Inf", 194, 14),
            new HeaderInfo("Mob_fee", 208, 8),
            new HeaderInfo("Toll_fee", 216, 8),
            new HeaderInfo("Inf_fee", 224, 8),
            new HeaderInfo("Pay_mode", 232, 1),
            new HeaderInfo("dis_id_1", 233, 32),
            new HeaderInfo("dis_id_2", 265, 32),
            new HeaderInfo("reserve", 297, 36),
            new HeaderInfo("cbe_flag", 333, 1),
            new HeaderInfo("period_flag", 334, 1),
            new HeaderInfo("SubsID", 335, 14),
            new HeaderInfo("A_pay_type", 349, 1),
            new HeaderInfo("A_pay_subno", 350, 24),
            new HeaderInfo("A_pay_switch_flag", 374, 2),
            new HeaderInfo("A_pay_district", 376, 2),
            new HeaderInfo("A_pay_brand", 378, 1),
            new HeaderInfo("A_pay_user_type", 379, 2),
            new HeaderInfo("A_AcctID", 381, 14),
            new HeaderInfo("A_deducted", 395, 1),
            new HeaderInfo("A_ACCT_BALANCE", 396, 12),
            new HeaderInfo("A_ACCT_BALANCE_ID1", 408, 18),
            new HeaderInfo("A_ACCT_BALANCE_AMT1", 426, 8),
            new HeaderInfo("A_ACCT_BALANCE_ID2", 434, 18),
            new HeaderInfo("A_ACCT_BALANCE_AMT2", 452, 8),
            new HeaderInfo("A_ACCT_BALANCE_ID3", 460, 18),
            new HeaderInfo("A_ACCT_BALANCE_AMT3", 478, 8),
            new HeaderInfo("A_ACCT_BALANCE_ID4", 486, 18),
            new HeaderInfo("A_ACCT_BALANCE_AMT4", 504, 8),
            new HeaderInfo("B_pay_type", 512, 1),
            new HeaderInfo("B_pay_subno", 513, 24),
            new HeaderInfo("B_pay_switch_flag", 537, 2),
            new HeaderInfo("B_pay_district", 539, 2),
            new HeaderInfo("B_pay_brand", 541, 1),
            new HeaderInfo("B_pay_user_type", 542, 2),
            new HeaderInfo("B_AcctID", 544, 14),
            new HeaderInfo("B_deducted", 558, 1),
            new HeaderInfo("B_ACCT_BALANCE", 559, 12),
            new HeaderInfo("B_ACCT_BALANCE_ID1", 571, 18),
            new HeaderInfo("B_ACCT_BALANCE_AMT1", 589, 8),
            new HeaderInfo("B_ACCT_BALANCE_ID2", 597, 18),
            new HeaderInfo("B_ACCT_BALANCE_AMT2", 615, 8),
            new HeaderInfo("B_ACCT_BALANCE_ID3", 623, 18),
            new HeaderInfo("B_ACCT_BALANCE_AMT3", 641, 8)};
    public static final HeaderInfo[] HEADER_BASIC_GROUP = {
            new HeaderInfo("EventFormatType", 0, 2),
            new HeaderInfo("roll_flag", 2, 1),
            new HeaderInfo("roll_count", 3, 2),
            new HeaderInfo("file_id", 5, 10),
            new HeaderInfo("exc_id", 15, 4),
            new HeaderInfo("FileType", 19, 2),
            new HeaderInfo("subno", 21, 24),
            new HeaderInfo("IMSI", 45, 15),
            new HeaderInfo("IMEI", 60, 15),
            new HeaderInfo("start_time", 75, 14),
            new HeaderInfo("group_flag_no", 89, 24), // In group add this new
            // field
            new HeaderInfo("special_flag", 113, 2),
            new HeaderInfo("proc_time", 115, 14),
            new HeaderInfo("event_id", 129, 20),
            new HeaderInfo("Switch_flag", 149, 2),
            new HeaderInfo("District", 151, 2),
            new HeaderInfo("Brand", 153, 1),
            new HeaderInfo("User_Type", 154, 2),
            new HeaderInfo("Visit_Area", 156, 8),
            new HeaderInfo("B_subno", 164, 24),
            new HeaderInfo("Bill_type", 188, 2),
            new HeaderInfo("ACCT_Mob", 190, 14),
            new HeaderInfo("ACCT_Toll", 204, 14),
            new HeaderInfo("ACCT_Inf", 218, 14),
            new HeaderInfo("Mob_fee", 232, 8),
            new HeaderInfo("Toll_fee", 240, 8),
            new HeaderInfo("Inf_fee", 248, 8),
            new HeaderInfo("Pay_mode", 256, 1),
            new HeaderInfo("dis_id", 257, 64),
            new HeaderInfo("reserve", 321, 36),
            new HeaderInfo("cbe_flag", 357, 1),
            new HeaderInfo("period_flag", 358, 1),
            new HeaderInfo("SubsID", 359, 14),
            new HeaderInfo("A_pay_type", 373, 1),
            new HeaderInfo("A_pay_subno", 374, 24),
            new HeaderInfo("A_pay_switch_flag", 398, 2),
            new HeaderInfo("A_pay_district", 400, 2),
            new HeaderInfo("A_pay_brand", 402, 1),
            new HeaderInfo("A_pay_user_type", 403, 2),
            new HeaderInfo("A_AcctID", 405, 14),
            new HeaderInfo("A_deducted", 419, 1),
            new HeaderInfo("A_ACCT_BALANCE", 420, 12),
            new HeaderInfo("A_ACCT_BALANCE_ID1", 432, 18),
            new HeaderInfo("A_ACCT_BALANCE_AMT1", 450, 8),
            new HeaderInfo("A_ACCT_BALANCE_ID2", 458, 18),
            new HeaderInfo("A_ACCT_BALANCE_AMT2", 476, 8),
            new HeaderInfo("A_ACCT_BALANCE_ID3", 484, 18),
            new HeaderInfo("A_ACCT_BALANCE_AMT3", 502, 8),
            new HeaderInfo("A_ACCT_BALANCE_ID4", 510, 18),
            new HeaderInfo("A_ACCT_BALANCE_AMT4", 528, 8),
            new HeaderInfo("B_pay_type", 536, 1),
            new HeaderInfo("B_pay_subno", 537, 24),
            new HeaderInfo("B_pay_switch_flag", 561, 2),
            new HeaderInfo("B_pay_district", 563, 2),
            new HeaderInfo("B_pay_brand", 565, 1),
            new HeaderInfo("B_pay_user_type", 566, 2),
            new HeaderInfo("B_AcctID", 568, 14),
            new HeaderInfo("B_deducted", 582, 1),
            new HeaderInfo("B_ACCT_BALANCE", 583, 12),
            new HeaderInfo("B_ACCT_BALANCE_ID1", 595, 18),
            new HeaderInfo("B_ACCT_BALANCE_AMT1", 613, 8),
            new HeaderInfo("B_ACCT_BALANCE_ID2", 621, 18),
            new HeaderInfo("B_ACCT_BALANCE_AMT2", 639, 8),
            new HeaderInfo("B_ACCT_BALANCE_ID3", 647, 18),
            new HeaderInfo("B_ACCT_BALANCE_AMT3", 665, 8)};
    public static final HeaderInfo[] HEADER_EXTENDED_VOICE = {
            new HeaderInfo("xd_type", 649, 1),
            new HeaderInfo("bus_code", 650, 15),
            new HeaderInfo("bus_type", 665, 2),
            new HeaderInfo("subbus_type", 667, 2),
            new HeaderInfo("rat_type", 669, 1),
            new HeaderInfo("dis_code", 670, 32),
            new HeaderInfo("direct_type", 702, 1),
            new HeaderInfo("visit_switch_flag", 703, 2),
            new HeaderInfo("roam_type", 705, 1),
            new HeaderInfo("toll_type", 706, 1),
            new HeaderInfo("carry_type", 707, 2),
            new HeaderInfo("b_operator", 709, 2),
            new HeaderInfo("b_switch_flag", 711, 2),
            new HeaderInfo("b_brand", 713, 1),
            new HeaderInfo("b_user_type", 714, 2),
            new HeaderInfo("dis_dura", 716, 7),
            new HeaderInfo("dis_fee", 723, 8),
            new HeaderInfo("Reserve2", 731, 30),
            new HeaderInfo("relation", 761, 2),
            new HeaderInfo("duration", 763, 7),
            new HeaderInfo("net_type", 770, 1),
            new HeaderInfo("multi_call", 771, 1),
            new HeaderInfo("call_type", 772, 2),
            new HeaderInfo("call_flag", 774, 2),
            new HeaderInfo("msisdnB", 776, 24),
            new HeaderInfo("msisdnC", 800, 24),
            new HeaderInfo("real_msisdn", 824, 24),
            new HeaderInfo("msrn", 848, 11), new HeaderInfo("msc_id", 859, 10),
            new HeaderInfo("calling_lac", 869, 4),
            new HeaderInfo("calling_cellid", 873, 8),
            new HeaderInfo("called_lac", 881, 4),
            new HeaderInfo("called_cellid", 885, 8),
            new HeaderInfo("out_router", 893, 21),
            new HeaderInfo("in_router", 914, 21),
            new HeaderInfo("service_type", 935, 3),
            new HeaderInfo("service_code", 938, 2),
            new HeaderInfo("session_id", 940, 16),
            new HeaderInfo("session_si", 956, 3),
            new HeaderInfo("session_type", 959, 1),
            new HeaderInfo("FCI", 960, 8),
            new HeaderInfo("vpn_call_type", 968, 2),
            new HeaderInfo("fee_one", 970, 8),
            new HeaderInfo("fee_two", 978, 8),
            new HeaderInfo("Reserve3", 986, 30)};
    // group voice
    public static final HeaderInfo[] HEADER_EXTENDED_VOICE_GROUP = {
            new HeaderInfo("xd_type", 673, 1),
            new HeaderInfo("bus_code", 674, 15),
            new HeaderInfo("bus_type", 689, 2),
            new HeaderInfo("subbus_type", 691, 2),
            new HeaderInfo("rat_type", 693, 1),
            new HeaderInfo("dis_code", 694, 32),
            new HeaderInfo("direct_type", 726, 1),
            new HeaderInfo("visit_switch_flag", 727, 2),
            new HeaderInfo("roam_type", 729, 1),
            new HeaderInfo("toll_type", 730, 1),
            new HeaderInfo("carry_type", 731, 2),
            new HeaderInfo("b_operator", 733, 2),
            new HeaderInfo("b_switch_flag", 735, 2),
            new HeaderInfo("b_brand", 737, 1),
            new HeaderInfo("b_user_type", 738, 2),
            new HeaderInfo("dis_dura", 740, 7),
            new HeaderInfo("dis_fee", 747, 8),
            new HeaderInfo("Reserve2", 755, 30),
            new HeaderInfo("relation", 785, 2),
            new HeaderInfo("duration", 787, 7),
            new HeaderInfo("net_type", 794, 1),
            new HeaderInfo("multi_call", 795, 1),
            new HeaderInfo("call_type", 796, 2),
            new HeaderInfo("call_flag", 798, 2),
            new HeaderInfo("msisdnB", 800, 24),
            new HeaderInfo("msisdnC", 824, 24),
            new HeaderInfo("real_msisdn", 848, 24),
            new HeaderInfo("msrn", 872, 11),
            new HeaderInfo("msc_id", 883, 10),
            new HeaderInfo("calling_lac", 893, 4),
            new HeaderInfo("calling_cellid", 897, 8),
            new HeaderInfo("called_lac", 905, 4),
            new HeaderInfo("called_cellid", 909, 8),
            new HeaderInfo("out_router", 917, 21),
            new HeaderInfo("in_router", 938, 21),
            new HeaderInfo("service_type", 959, 3),
            new HeaderInfo("service_code", 962, 2),
            new HeaderInfo("session_id", 964, 64), // Change 16 to 64
            new HeaderInfo("session_si", 1028, 3),
            new HeaderInfo("session_type", 1031, 1),
            new HeaderInfo("FCI", 1032, 8),
            new HeaderInfo("vpn_call_type", 1040, 2),
            new HeaderInfo("fee_one", 1042, 8),
            new HeaderInfo("fee_two", 1050, 8),
            new HeaderInfo("Reserve3", 1058, 30)};
    // sms
    public static final HeaderInfo[] HEADER_EXTENDED_SMS = {

            new HeaderInfo("xd_type", 649, 1), new HeaderInfo("bus_code", 650, 15),
            new HeaderInfo("bus_type", 665, 2),
            new HeaderInfo("subbus_type", 667, 2),
            new HeaderInfo("rat_type", 669, 1),
            new HeaderInfo("dis_code", 670, 32),
            new HeaderInfo("direct_type", 702, 1),
            new HeaderInfo("visit_switch_flag", 703, 2),
            new HeaderInfo("roam_type", 705, 1),
            new HeaderInfo("b_operator", 706, 2),
            new HeaderInfo("b_switch_flag", 708, 2),
            new HeaderInfo("b_brand", 710, 1),
            new HeaderInfo("b_user_type", 711, 2),
            new HeaderInfo("icp_stat", 713, 2),
            new HeaderInfo("dis_dura", 715, 7),
            new HeaderInfo("dis_fee", 722, 8),
            new HeaderInfo("Reserve2", 730, 30),
            new HeaderInfo("relation", 760, 2),
            new HeaderInfo("msisdnB", 762, 24),
            new HeaderInfo("real_msisdn", 786, 24),
            new HeaderInfo("cdr_tag", 810, 2),
            new HeaderInfo("srv_tag", 812, 2),
            new HeaderInfo("direct_tag", 814, 1),
            new HeaderInfo("vpmn_tag", 815, 1),
            new HeaderInfo("sp_code", 816, 20),
            new HeaderInfo("op_code", 836, 30),
            new HeaderInfo("cdr_fee", 866, 8),
            new HeaderInfo("chrg_type", 874, 2),
            new HeaderInfo("duration", 876, 7),
            new HeaderInfo("srv_type", 883, 10),
            new HeaderInfo("channel_id", 893, 8),
            new HeaderInfo("extend_one", 901, 2),
            new HeaderInfo("extend_two", 903, 30),
            new HeaderInfo("Reserve3", 933, 30)};
    // groups sms
    public static final HeaderInfo[] HEADER_EXTENDED_SMS_GROUP = {
            new HeaderInfo("xd_type", 673, 1),
            new HeaderInfo("bus_code", 674, 15),
            new HeaderInfo("bus_type", 689, 2),
            new HeaderInfo("subbus_type", 691, 2),
            new HeaderInfo("rat_type", 693, 1),
            new HeaderInfo("dis_code", 694, 32),
            new HeaderInfo("direct_type", 726, 1),
            new HeaderInfo("visit_switch_flag", 727, 2),
            new HeaderInfo("roam_type", 729, 1),
            new HeaderInfo("b_operator", 730, 2),
            new HeaderInfo("b_switch_flag", 732, 2),
            new HeaderInfo("b_brand", 734, 1),
            new HeaderInfo("b_user_type", 735, 2),
            new HeaderInfo("icp_stat", 737, 2),
            new HeaderInfo("dis_dura", 739, 7),
            new HeaderInfo("dis_fee", 746, 8),
            new HeaderInfo("Reserve2", 754, 30),
            new HeaderInfo("relation", 784, 2),
            new HeaderInfo("msisdnB", 786, 24),
            new HeaderInfo("real_msisdn", 810, 24),
            new HeaderInfo("cdr_tag", 834, 2),
            new HeaderInfo("srv_tag", 836, 2),
            new HeaderInfo("direct_tag", 838, 1),
            new HeaderInfo("vpmn_tag", 839, 1),
            new HeaderInfo("sp_code", 840, 20),
            new HeaderInfo("op_code", 860, 30),
            new HeaderInfo("cdr_fee", 890, 8),
            new HeaderInfo("chrg_type", 898, 2),
            new HeaderInfo("duration", 900, 7),
            new HeaderInfo("srv_type", 907, 10),
            new HeaderInfo("channel_id", 917, 8),
            new HeaderInfo("extend_one", 925, 2),
            new HeaderInfo("extend_two", 927, 30),
            new HeaderInfo("Reserve3", 957, 30)};
    // GPRS
    public static final HeaderInfo[] HEADER_EXTENDED_GPRS = {
            new HeaderInfo("xd_type", 649, 1),
            new HeaderInfo("bus_code", 650, 15),
            new HeaderInfo("bus_type", 665, 2),
            new HeaderInfo("subbus_type", 667, 2),
            new HeaderInfo("rat_type", 669, 1),
            new HeaderInfo("dis_code", 670, 32),
            new HeaderInfo("visit_switch_flag", 702, 2),
            new HeaderInfo("roam_type", 704, 1),
            new HeaderInfo("b_operator", 705, 2),
            new HeaderInfo("dis_dura", 707, 12),
            new HeaderInfo("dis_fee", 719, 8),
            new HeaderInfo("Reserve2", 727, 30),
            new HeaderInfo("apn_switch_flag", 757, 2),
            new HeaderInfo("rg_code", 759, 10),
            new HeaderInfo("dur_vol_flag", 769, 1),
            new HeaderInfo("apn_type", 770, 1),
            new HeaderInfo("top_flag_fee", 771, 1),
            new HeaderInfo("charging_id", 772, 10),
            new HeaderInfo("real_msisdn", 782, 24),
            new HeaderInfo("apn_name_ni", 806, 64),
            new HeaderInfo("ggsn_addr", 870, 32),
            new HeaderInfo("sgsn_addr", 902, 32),
            new HeaderInfo("duration", 934, 7),
            new HeaderInfo("sequence", 941, 10),
            new HeaderInfo("service_code", 951, 10),
            new HeaderInfo("up_volume", 961, 11),
            new HeaderInfo("down_volume", 972, 11),
            new HeaderInfo("u_fee", 983, 8),
            new HeaderInfo("net_type", 991, 1),
            new HeaderInfo("visit_area_code", 992, 9),
            new HeaderInfo("visited_carrier_cd", 1001, 5),
            new HeaderInfo("partial_typeid", 1006, 1),
            new HeaderInfo("Reserve3", 1007, 30)};
    // group GPRS
    public static final HeaderInfo[] HEADER_EXTENDED_GPRS_GROUP = {
            new HeaderInfo("xd_type", 673, 1),
            new HeaderInfo("bus_code", 674, 15),
            new HeaderInfo("bus_type", 689, 2),
            new HeaderInfo("subbus_type", 691, 2),
            new HeaderInfo("rat_type", 693, 1),
            new HeaderInfo("dis_code", 694, 32),
            new HeaderInfo("visit_switch_flag", 726, 2),
            new HeaderInfo("roam_type", 728, 1),
            new HeaderInfo("b_operator", 729, 2),
            new HeaderInfo("dis_dura", 731, 12),
            new HeaderInfo("dis_fee", 743, 8),
            new HeaderInfo("Reserve2", 751, 30),
            new HeaderInfo("b_switch_flag", 781, 2),
            new HeaderInfo("rg_code", 783, 10),
            new HeaderInfo("dur_vol_flag", 793, 1),
            new HeaderInfo("b_user_type", 794, 2),
            new HeaderInfo("b_brand", 796, 1),
            new HeaderInfo("top_flag_fee", 797, 1),
            new HeaderInfo("charging_id", 798, 10),
            new HeaderInfo("real_msisdn", 808, 24),
            new HeaderInfo("apn_name_ni", 832, 64),
            new HeaderInfo("ggsn_addr", 896, 32),
            new HeaderInfo("sgsn_addr", 928, 32),
            new HeaderInfo("duration", 960, 7),
            new HeaderInfo("sequence", 967, 10),
            new HeaderInfo("service_code", 977, 21),
            new HeaderInfo("up_volume", 998, 11),
            new HeaderInfo("down_volume", 1009, 11),
            new HeaderInfo("u_fee", 1020, 8),
            new HeaderInfo("net_type", 1028, 1),
            new HeaderInfo("visit_area_code", 1029, 9),
            new HeaderInfo("visited_carrier_cd", 1038, 5),
            new HeaderInfo("partial_typeid", 1043, 1),
            new HeaderInfo("Reserve3", 1044, 30)};
    // special
    public static final HeaderInfo[] HEADER_EXTENDED_SPECIAL = {
            new HeaderInfo("xd_type", 649, 1),
            new HeaderInfo("bus_code", 650, 15),
            new HeaderInfo("bus_type", 665, 2),
            new HeaderInfo("subbus_type", 667, 2),
            new HeaderInfo("rat_type", 669, 1),
            new HeaderInfo("dis_code", 670, 32),
            new HeaderInfo("direct_type", 702, 1),
            new HeaderInfo("visit_switch_flag", 703, 2),
            new HeaderInfo("roam_type", 705, 1),
            new HeaderInfo("toll_type", 706, 1),
            new HeaderInfo("carry_type", 707, 2),
            new HeaderInfo("b_operator", 709, 2),
            new HeaderInfo("b_switch_flag", 711, 2),
            new HeaderInfo("b_brand", 713, 1),
            new HeaderInfo("b_user_type", 714, 2),
            new HeaderInfo("dis_dura", 716, 7),
            new HeaderInfo("dis_fee", 723, 8),
            new HeaderInfo("Reserve2", 731, 30),
            new HeaderInfo("duration", 761, 7),
            new HeaderInfo("msisdnA", 768, 24),
            new HeaderInfo("msisdnB", 792, 24),
            new HeaderInfo("msisdnC", 816, 24),
            new HeaderInfo("real_msisdn", 840, 24),
            new HeaderInfo("cdr_type", 864, 2),
            new HeaderInfo("call_type", 866, 2),
            new HeaderInfo("service_code", 868, 12),
            new HeaderInfo("ServType", 880, 3),
            new HeaderInfo("plan_mode", 883, 2),
            new HeaderInfo("call_flag", 885, 1),
            new HeaderInfo("roam_area", 886, 4),
            new HeaderInfo("cdr_mob_fee", 890, 8),
            new HeaderInfo("cdr_toll_fee", 898, 8),
            new HeaderInfo("cdr_inf_fee", 906, 8),
            new HeaderInfo("Reserve3", 914, 30)};
    // WLAN
    public static final HeaderInfo[] HEADER_EXTENDED_WLAN = {
            new HeaderInfo("xd_type", 649, 1),
            new HeaderInfo("bus_code", 650, 15),
            new HeaderInfo("bus_type", 665, 2),
            new HeaderInfo("subbus_type", 667, 2),
            new HeaderInfo("rat_type", 669, 1),
            new HeaderInfo("dis_code", 670, 32),
            new HeaderInfo("visit_switch_flag", 702, 2),
            new HeaderInfo("roam_type", 704, 1),
            new HeaderInfo("b_operator", 705, 2),
            new HeaderInfo("dis_dura", 707, 7),
            new HeaderInfo("dis_fee", 714, 8),
            new HeaderInfo("Reserve2", 722, 30),
            new HeaderInfo("area_id", 752, 9),
            new HeaderInfo("dur_vol_flag", 761, 1),
            new HeaderInfo("top_flag_fee", 762, 1),
            new HeaderInfo("cdr_roam_type", 763, 2),
            new HeaderInfo("real_msisdn", 765, 24),
            new HeaderInfo("duration", 789, 7),
            new HeaderInfo("up_volume", 796, 13),
            new HeaderInfo("down_volume", 809, 13),
            new HeaderInfo("hotspot_id", 822, 24),
            new HeaderInfo("logic_port_no", 846, 64),
            new HeaderInfo("cause_close", 910, 10),
            new HeaderInfo("user_ip_type", 920, 1),
            new HeaderInfo("user_ip", 921, 32),
            new HeaderInfo("cooperator_no", 953, 30),
            new HeaderInfo("basic_fee", 983, 8),
            new HeaderInfo("Reserve3", 991, 30)};
    // group WLAN
    public static final HeaderInfo[] HEADER_EXTENDED_WLAN_GROUP = {
            new HeaderInfo("xd_type", 673, 1),
            new HeaderInfo("bus_code", 674, 15),
            new HeaderInfo("bus_type", 689, 2),
            new HeaderInfo("subbus_type", 691, 2),
            new HeaderInfo("rat_type", 693, 1),
            new HeaderInfo("dis_code", 694, 32),
            new HeaderInfo("visit_switch_flag", 726, 2),
            new HeaderInfo("roam_type", 728, 1),
            new HeaderInfo("b_operator", 729, 2),
            new HeaderInfo("dis_dura", 731, 7),
            new HeaderInfo("dis_fee", 738, 8),
            new HeaderInfo("Reserve2", 746, 30),
            new HeaderInfo("area_id", 776, 9),
            new HeaderInfo("dur_vol_flag", 785, 1),
            new HeaderInfo("top_flag_fee", 786, 1),
            new HeaderInfo("cdr_roam_type", 787, 2),
            new HeaderInfo("real_msisdn", 789, 24),
            new HeaderInfo("duration", 813, 7),
            new HeaderInfo("up_volume", 820, 13),
            new HeaderInfo("down_volume", 833, 13),
            new HeaderInfo("hotspot_id", 846, 24),
            new HeaderInfo("logic_port_no", 870, 64),
            new HeaderInfo("cause_close", 934, 10),
            new HeaderInfo("user_ip_type", 944, 1),
            new HeaderInfo("user_ip", 945, 32),
            new HeaderInfo("cooperator_no", 977, 30),
            new HeaderInfo("basic_fee", 1007, 8),
            new HeaderInfo("Reserve3", 1015, 30)};
    // FIXED_BILL
    public static final HeaderInfo[] HEADER_EXTENDED_FIXED_BILL = {
            new HeaderInfo("EventFormatType", 0, 2),
            new HeaderInfo("roll_flag", 2, 1),
            new HeaderInfo("roll_count", 3, 2),
            new HeaderInfo("file_id", 5, 10), new HeaderInfo("exc_id", 15, 4),
            new HeaderInfo("subno", 19, 24),
            new HeaderInfo("proc_time", 43, 14),
            new HeaderInfo("Switch_flag", 57, 2),
            new HeaderInfo("Brand", 59, 1), new HeaderInfo("Bill_type", 60, 2),
            new HeaderInfo("acctid", 62, 14), new HeaderInfo("fee", 76, 12),
            new HeaderInfo("acct_balance_b", 88, 12),
            new HeaderInfo("Reserve", 100, 30),
            new HeaderInfo("xd_type", 130, 1),
            new HeaderInfo("Show_time", 131, 14),
            new HeaderInfo("Begin_day", 145, 8),
            new HeaderInfo("End_day", 153, 8),
            new HeaderInfo("prodid", 161, 32), new HeaderInfo("op_id", 193, 5),
            new HeaderInfo("Reserve2", 198, 100)};
    // OTHER_BILL
    public static final HeaderInfo[] HEADER_EXTENDED_OTHER_BILL = {
            new HeaderInfo("EventFormatType", 0, 2),
            new HeaderInfo("roll_flag", 2, 1),
            new HeaderInfo("roll_count", 3, 2),
            new HeaderInfo("file_id", 5, 10), new HeaderInfo("exc_id", 15, 4),
            new HeaderInfo("subno", 19, 24),
            new HeaderInfo("proc_time", 43, 14),
            new HeaderInfo("Switch_flag", 57, 2),
            new HeaderInfo("Brand", 59, 1), new HeaderInfo("Bill_type", 60, 2),
            new HeaderInfo("acctid", 62, 14), new HeaderInfo("fee", 76, 12),
            new HeaderInfo("acct_balance_b", 88, 12),
            new HeaderInfo("Reserve", 100, 30),
            new HeaderInfo("xd_type", 130, 1),
            new HeaderInfo("bus_code", 131, 15),
            new HeaderInfo("Show_time", 146, 14),
            new HeaderInfo("Fee_type", 160, 4),
            new HeaderInfo("pay_subno", 164, 24),
            new HeaderInfo("pay_switch_flag", 188, 2),
            new HeaderInfo("pay_brand", 190, 1),
            new HeaderInfo("Dis_fee", 191, 1),
            new HeaderInfo("Special_user", 192, 2),
            new HeaderInfo("object", 194, 4),
            new HeaderInfo("Special_subno", 198, 24),
            new HeaderInfo("sp_code", 222, 20),
            new HeaderInfo("multi_call", 242, 1),
            new HeaderInfo("Reserve2", 243, 60)};
    public static final HeaderInfo[][] HEADER_EXTENDED = {
            null, // 0 dummy
            HEADER_EXTENDED_VOICE, HEADER_EXTENDED_SMS, HEADER_EXTENDED_GPRS,
            HEADER_EXTENDED_SPECIAL, HEADER_EXTENDED_WLAN,
            HEADER_EXTENDED_FIXED_BILL, HEADER_EXTENDED_OTHER_BILL, null,
            null,
            null, // 8, 9 , 10 dummy
            HEADER_EXTENDED_VOICE_GROUP, HEADER_EXTENDED_SMS_GROUP,
            HEADER_EXTENDED_GPRS, null, // 14 dummy
            HEADER_EXTENDED_WLAN};
    public static String TABLENAME_PREFIX = null;
    public static String storeEncoding;
    public static boolean writeToWAL = true;
    public static int MAX_SEQ_LENGTH = 9;
    private static byte[][] _splitKeys;
    private String _phoneNum;
    private String _startTime;
    private String _flag;
    private int _flag_i = 0;
    private String _eventFormatType;
    private int _eventFormatType_i;
    private String _area;
    private String _fileId;
    private String _rowBasic;
    private String _rowExtended;
    private String _rowBasicDelimited;
    private String _rowExtendedDelimited;
    private String _rowCommonDelimited;
    private String _rowBasicDelimitedAppendix;
    private int _seq = 0;

    public BOSSRecordBack() {
    }

    public BOSSRecordBack(String phoneNum, String startTime, String flag,
                          String eventFormatType, String rowBasic, String rowExtended) {
        _phoneNum = phoneNum;
        _startTime = startTime;
        _flag = flag;
        _eventFormatType = eventFormatType;
        _rowBasic = rowBasic;
        _rowExtended = rowExtended;
    }

    public HeaderInfo[] getHeaderBasic() {
        if (_eventFormatType_i == 1 || _eventFormatType_i == 2
                || _eventFormatType_i == 3 || _eventFormatType_i == 4
                || _eventFormatType_i == 5 || _eventFormatType_i == 6
                || _eventFormatType_i == 7) {
            return HEADER_BASIC;
        } else {
            return HEADER_BASIC_GROUP;
        }
    }

    public HeaderInfo[] getHeaderExtended() {
        return HEADER_EXTENDED[_eventFormatType_i];
    }

    private String generateDelimited(byte[] bytes, HeaderInfo[] hi)
            throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hi.length; i++) {
            String str = new String(Arrays.copyOfRange(bytes, hi[i].getStart(),
                    hi[i].getStart() + hi[i].getLength()), storeEncoding);
            sb.append(str);
            if (i != hi.length - 1) {
                sb.append(DELIMITER);
            }
        }
        return sb.toString();
    }

    public String getFileId() {
        return _fileId;
    }

    public void setFileId(String fileId) {
        _fileId = fileId;
    }

    public int getFlag_i() {
        return _flag_i;
    }

    public void setFlag_i(int flag_i) {
        this._flag_i = flag_i;
        // _flag = Integer.toString(_flag_i);
        String str = Integer.toString(_flag_i);
        switch (str.length()) {
            case 1:
                _flag = "000" + str;
                break;
            case 2:
                _flag = "00" + str;
                break;
            case 3:
                _flag = "0" + str;
                break;
            default:
                _flag = str;
        }
    }

    public String getSeqString(int seq) {
        String seqString = Integer.toString(seq);
        StringBuilder ret = new StringBuilder();
        int shift = MAX_SEQ_LENGTH - seqString.length();

        for (int i = 0; i < shift; i++) {
            ret.append("0");
        }
        ret.append(seqString);
        return ret.toString();
    }

    @Override
    public byte[][] getColumnFamilies() {
        return new byte[][]{FAMILY_NAME};
    }

    @Override
    public HBaseColumn[] getHBaseColumns() {

        return HBASE_COLUMNS[_eventFormatType_i];
    }

    @Override
    public String getHBaseColumnValue(String family, String column) {
        if (family.equals("Info")) {
            if (column.equals("EventFormatType")) {
                return _eventFormatType;
            } else if (column.equals("Area")) {
                return _area;
            } else if (column.equals("CBE_Basic")) {
                // return _rowBasicDelimited;
                return _rowBasicDelimited + _rowBasicDelimitedAppendix;
            } else if (column.equals("CBE_voice") || column.equals("CBE_sms")
                    || column.equals("CBE_gprs")
                    || column.equals("CBE_special")
                    || column.equals("CBE_wlan") || column.equals("Fixed_Bill")
                    || column.equals("Other_Bill")) {
                return _rowExtendedDelimited;
            } else if (column.equals("Common")) {
                return _rowCommonDelimited;
            }
        }
        return null;
    }

    @Override
    public void setStoreEncoding(String encoding) {
        storeEncoding = encoding;
    }

    /**
     * @param family
     * @param column
     * @return Always return UTF-8 encoded bytes
     */
    @Override
    public byte[] getHBaseColumnValueInBytes(String family, String column)
            throws IOException {
        String val = getHBaseColumnValue(family, column);
        if (val != null) {
            return Bytes.toBytes(val);
        }
        return null;
    }

    @Override
    public String getHBaseRowKey() {
        StringBuilder sb = new StringBuilder();
        sb.append(getPhoneNum());
        sb.append(DELIMITER);
        sb.append(getStartTime());
        sb.append(DELIMITER);
        String flag = getFlag();
        if (flag != null) { // multiple records with the same phone and
            // startTime, append sequence no
            sb.append(flag);
        }
        sb.append(DELIMITER);
        sb.append(_fileId);
        sb.append(DELIMITER);
        sb.append(getSeqString(_seq));

        return sb.toString();
    }

    public void parseString(byte[] bytes) throws UnsupportedEncodingException {
        _eventFormatType = new String(Arrays.copyOfRange(bytes, 0, 2),
                storeEncoding);
        _eventFormatType_i = Integer.parseInt(_eventFormatType);

        HeaderInfo[] headerExtended = getHeaderExtended();
        int temp = headerExtended.length - 1;
        int extendedLast = headerExtended[temp].getStart()
                + headerExtended[temp].getLength();

        if (_eventFormatType_i == 1 || _eventFormatType_i == 2
                || _eventFormatType_i == 3 || _eventFormatType_i == 4
                || _eventFormatType_i == 5) { // the first 5 event format type
            _area = new String(Arrays.copyOfRange(bytes, 125, 125 + 2),
                    storeEncoding);
            _phoneNum = new String(Arrays.copyOfRange(bytes, 21,
                    21 + PHONE_NUM_LENGTH), storeEncoding);
            _startTime = new String(Arrays.copyOfRange(bytes, 75,
                    75 + START_TIME_LENGTH), storeEncoding);

            _rowBasic = new String(Arrays.copyOfRange(bytes, 0, 649),
                    storeEncoding);

            // generate Appendix
            _rowBasicDelimited = generateDelimited(bytes, HEADER_BASIC);

            _rowExtended = new String(Arrays.copyOfRange(bytes, 649,
                    extendedLast), storeEncoding);

            _rowCommonDelimited = generateDelimited(bytes, HEADER_COMMON_CBE);

        } else if (_eventFormatType_i == 6 || _eventFormatType_i == 7) { // event
            // format
            // type
            // is
            // 6,
            // 7
            _area = new String(Arrays.copyOfRange(bytes, 57, 57 + 2),
                    storeEncoding);
            _phoneNum = new String(Arrays.copyOfRange(bytes, 19,
                    19 + PHONE_NUM_LENGTH), storeEncoding);
            if (_eventFormatType_i == 6) {
                _startTime = new String(Arrays.copyOfRange(bytes, 131,
                        131 + START_TIME_LENGTH), storeEncoding);
            } else if (_eventFormatType_i == 7) {
                _startTime = new String(Arrays.copyOfRange(bytes, 146,
                        146 + START_TIME_LENGTH), storeEncoding);
            }
            _rowBasic = null;
            _rowBasicDelimited = null;

            // System.err.println("LENGTH========"+rawRecord.length());
            _rowExtended = new String(
                    Arrays.copyOfRange(bytes, 0, extendedLast), storeEncoding);

            _rowCommonDelimited = generateDelimited(bytes, HEADER_COMMON_BILL);
        } else if (_eventFormatType_i == 11 || _eventFormatType_i == 12
                || _eventFormatType_i == 13 || _eventFormatType_i == 15) {
            _area = new String(Arrays.copyOfRange(bytes, 149, 149 + 2),
                    storeEncoding);
            _phoneNum = new String(Arrays.copyOfRange(bytes, 21,
                    21 + PHONE_NUM_LENGTH), storeEncoding);
            _startTime = new String(Arrays.copyOfRange(bytes, 75,
                    75 + START_TIME_LENGTH), storeEncoding);

            _rowBasic = new String(Arrays.copyOfRange(bytes, 0, 673),
                    storeEncoding);

            // generate Appendix
            _rowBasicDelimited = generateDelimited(bytes, HEADER_BASIC_GROUP);

            _rowExtended = new String(Arrays.copyOfRange(bytes, 673,
                    extendedLast), storeEncoding);

            _rowCommonDelimited = generateDelimited(bytes,
                    HEADER_COMMON_CBE_GROUP);
        }

        _rowExtendedDelimited = generateDelimited(bytes, headerExtended);
    }

    public String getPhoneNum() {
        return _phoneNum;
    }

    public int getSeq() {
        return _seq;
    }

    public String getStartTime() {
        return _startTime;
    }

    public String getFlag() {
        return _flag;
    }

    public String getEventFormatType() {
        return _eventFormatType;
    }

    public String getRowBasic() {
        return _rowBasic;
    }

    public String getRowExtended() {
        return _rowExtended;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PHONE_NUM => '");
        sb.append(_phoneNum);
        sb.append("', ");
        sb.append("START_TIME => '");
        sb.append(_startTime);
        sb.append("', ");
        sb.append("FLAG => '");
        sb.append(_flag);
        sb.append("', ");
        sb.append("EVENT_FORMAT_TYPE => '");
        sb.append(_eventFormatType);
        sb.append("', ");

        sb.append("ROW_BASIC => ").append(_rowBasic);
        sb.append("ROW_EXTENDED => ").append(_rowExtended);
        /*
		 * sb.append("ROW_BASIC => [{"); if (_rowBasic != null) { for (int i =
		 * 0; i < HEADER_BASIC.length; i++) { if (i != 0) { sb.append(", "); }
		 * sb.append(Bytes.toString(FAMILY_BASIC) +
		 * Bytes.toString(HEADER_BASIC[i].getName())); sb.append(" => ");
		 * sb.append(Bytes.toString(_rowBasic[i])); } } sb.append("}]");
		 */

        return sb.toString();
    }

    @Override
    public void skipFileHeader(BufferedReader reader, String fileName)
            throws IOException {
        try {
            _fileId = fileName.split("\\.")[2];
        } catch (Exception e) {
            throw new IOException("Invalid BOSSRecord file name: " + fileName);
        }
        reader.readLine();
        setFlag_i(-1);
    }

    @Override
    public boolean parse(BufferedReader reader, Record previous)
            throws IOException, ParseException {
        String rawRecord = reader.readLine();
        // TEST ONLY
        // if(rawRecord != null) return true;
        if (rawRecord == null) {
            return false;
        }
        try {
            parseString(rawRecord.getBytes(storeEncoding));
        } catch (UnsupportedEncodingException e) {
            throw new ParseException("Unsupported encoding method: "
                    + storeEncoding, e);
        } catch (StringIndexOutOfBoundsException e) {
            // System.err.println("Invalid BOSSRecord (length: " +
            // rawRecord.length() + "): " + rawRecord);
            throw new ParseException("Invalid BOSSRecord(length:"
                    + rawRecord.length() + "): " + rawRecord, e);
        } catch (Exception e) {
            // System.err.println("Invalid BOSSRecord: " + rawRecord);
            throw new ParseException("Invalid BOSSRecord: " + rawRecord, e);
        }

        if (previous != null
                && this.getPhoneNum().equals(
                ((BOSSRecordBack) previous).getPhoneNum())
                && this.getStartTime().equals(
                ((BOSSRecordBack) previous).getStartTime())) {
            this.setFlag_i(((BOSSRecordBack) previous).getFlag_i() + 1);
        } else {
            this.setFlag_i(0);
        }

        if (previous != null) {
            this.setFileId(((BOSSRecordBack) previous).getFileId());
            this._seq = ((BOSSRecordBack) previous).getSeq() + 1;
        } else {
            this._seq = 0;
        }

        return true;
    }

    @Override
    public void setTableNamePrefix(String prefix) {
        TABLENAME_PREFIX = prefix;
    }

    @Override
    public String getTableNameFromFileName(String fileName) throws IOException {
        // boss.EventFormatType.fileid.channelid1.channelid2.yyyymm.yyyymmdd.excid.mark.Z
        try {
            String yearMonth = fileName.split("\\.")[5].substring(0, 6);
            Integer.parseInt(yearMonth);
            return TABLENAME_PREFIX + yearMonth;
        } catch (Exception e) {
            throw new IOException("Invalid BOSSRecord file name: " + fileName);
        }
    }

    @Override
    public void constructSplitKeys(String[] prefixes, int splitSize) {
        _splitKeys = genSplitKeys(prefixes, splitSize);
    }

    @Override
    public byte[][] getSplitKeys() {
        return _splitKeys;
    }

    @Override
    public boolean getWriteToWAL() {
        return writeToWAL;
    }

    @Override
    public void setWriteToWAL(boolean write) {
        writeToWAL = write;
    }

    @Override
    public String getStringField(String field) {
        if (field.equals("subno")) {
            return getPhoneNum();
        }
        return "";
    }
}
