SELECT 
   covariance(C_19_00025, C_17_00021) / 
    (stddev(C_19_00025)*stddev(C_17_00021)) correlation_25_21,
   covariance(C_14_00035, C_17_00021) / 
    (stddev(C_14_00035)*stddev(C_17_00021)) correlation_35_21,
   covariance(C_15_00036, C_17_00021) / 
    (stddev(C_15_00036)*stddev(C_17_00021)) correlation_36_21,
   covariance(C_03_00045, C_17_00021) / 
    (stddev(C_03_00045)*stddev(C_17_00021)) correlation_45_21,
   covariance(C_18_00052, C_17_00021) / 
    (stddev(C_18_00052)*stddev(C_17_00021)) correlation_52_21,
   covariance(C_28_62608, C_17_00021) / 
    (stddev(C_28_62608)*stddev(C_17_00021)) correlation_62608_21,
   covariance(C_14_00035, C_19_00025) / 
    (stddev(C_14_00035)*stddev(C_19_00025)) correlation_35_25,
   covariance(C_15_00036, C_19_00025) / 
    (stddev(C_15_00036)*stddev(C_19_00025)) correlation_36_25,
   covariance(C_26_00045, C_19_00025) / 
    (stddev(C_26_00045)*stddev(C_19_00025)) correlation_45_25,
   covariance(C_18_00052, C_19_00025) / 
    (stddev(c_18_00052)*stddev(C_19_00025)) correlation_52_25,
   covariance(c_28_62608, C_19_00025) / 
    (stddev(c_28_62608)*stddev(C_19_00025)) correlation_62608_25,
   covariance(C_15_00036, C_14_00035) / 
    (stddev(C_15_00036)*stddev(C_14_00035)) correlation_36_35,
   covariance(C_03_00045, C_14_00035) / 
    (stddev(C_03_00045)*stddev(C_14_00035)) correlation_45_35,
   covariance(c_18_00052, C_14_00035) / 
    (stddev(c_18_00052)*stddev(C_14_00035)) correlation_52_35,
   covariance(c_28_62608, C_14_00035) / 
    (stddev(c_28_62608)*stddev(C_14_00035)) correlation_62608_35,
   covariance(C_03_00045, c_15_00036) / 
    (stddev(C_03_00045)*stddev(c_15_00036)) correlation_45_36,
   covariance(c_18_00052, c_15_00036) / 
    (stddev(c_18_00052)*stddev(c_15_00036)) correlation_52_36,
   covariance(c_28_62608, c_15_00036) / 
    (stddev(c_28_62608)*stddev(c_15_00036)) correlation_62608_36,
   covariance(c_18_00052, c_03_00045) / 
    (stddev(c_18_00052)*stddev(c_03_00045)) correlation_52_45,
   covariance(c_28_62608, c_03_00045) / 
    (stddev(c_28_62608)*stddev(c_03_00045)) correlation_62608_45,
   covariance(c_28_62608, c_18_00052) / 
    (stddev(c_28_62608)*stddev(c_18_00052)) correlation_62608_52   
FROM 
  T_50999999;
