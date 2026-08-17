SET threads=1; SET aligned_data_root='D:/proj/factorlake/testdata'; SELECT rowid FROM aligned_table('cnstk_ixday') WHERE rowid < 100 LIMIT 3;
