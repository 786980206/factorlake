SET aligned_data_root='D:/proj/factorlake/testdata_perf';
ATTACH 'D:/proj/factorlake/testdata_perf' AS al (TYPE ALIGNED);
SELECT * FROM aligned_groups('perf_test', 'D:/proj/factorlake/testdata_perf');
