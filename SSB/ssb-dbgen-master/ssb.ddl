DROP TABLE CUSTOMER;
DROP TABLE SSB.DATES;
DROP TABLE PART; 
DROP TABLE SUPPLIER;
DROP TABLE LINEORDER;

CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL PRIMARY KEY,
                            C_NAME        VARCHAR(25) NOT NULL,
                            C_ADDRESS     VARCHAR(40) NOT NULL,
                            C_CITY        VARCHAR(10) NOT NULL,
                            C_NATION      VARCHAR(15) NOT NULL,
                            C_REGION      VARCHAR(12) NOT NULL,
                            C_PHONE       VARCHAR(15) NOT NULL,
                            C_MKTSEGMENT  VARCHAR(10) NOT NULL);

CREATE TABLE SSB.DATES ( D_DATEKEY          INTEGER PRIMARY KEY,
                         D_DATE             VARCHAR(18) NOT NULL,
                         D_DAYOFWEEK        VARCHAR(8) NOT NULL,
                         D_MONTH            VARCHAR(9) NOT NULL,
                         D_YEAR             INTEGER NOT NULL,
                         D_YEARMONTHNUM     INTEGER,
                         D_YEARMONTH        VARCHAR(7) NOT NULL,
                         D_DAYNUMINWEEK     INTEGER,
                         D_DAYNUMINMONTH    INTEGER,
                         D_DAYNUMINYEAR     INTEGER,
                         D_MONTHNUMINYEAR   INTEGER,
                         D_WEEKNUMINYEAR    INTEGER,
                         D_SELLINGSEASON    VARCHAR(12) NOT NULL,
                         D_LASTDAYINWEEKFL  INTEGER,
                         D_LASTDAYINMONTHFL INTEGER,
                         D_HOLIDAYFL        INTEGER,
                         D_WEEKDAYFL        INTEGER);
                         
CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL PRIMARY KEY,
                         P_NAME        VARCHAR(22) NOT NULL,
                         P_MFGR        VARCHAR(6) NOT NULL,
                         P_CATEGORY    VARCHAR(7) NOT NULL,
                         P_BRAND       VARCHAR(9) NOT NULL,
                         P_COLOR       VARCHAR(11) NOT NULL,
                         P_TYPE        VARCHAR(25) NOT NULL,
                         P_SIZE        INTEGER NOT NULL,
                         P_CONTAINER   VARCHAR(10) NOT NULL);

CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL PRIMARY KEY,
                            S_NAME        VARCHAR(25) NOT NULL,
                            S_ADDRESS     VARCHAR(25) NOT NULL,
                            S_CITY        VARCHAR(10) NOT NULL,
                            S_NATION      VARCHAR(15) NOT NULL,
                            S_REGION      VARCHAR(12) NOT NULL,
                            S_PHONE       VARCHAR(15) NOT NULL);

CREATE TABLE LINEORDER ( LO_ORDERKEY       BIGINT,
                             LO_LINENUMBER     BIGINT,
                             LO_CUSTKEY        INTEGER NOT NULL,
                             LO_PARTKEY        INTEGER NOT NULL,
                             LO_SUPPKEY        INTEGER NOT NULL,
                             LO_ORDERDATE      INTEGER NOT NULL,
                             LO_ORDERPRIOTITY  VARCHAR(15) NOT NULL,
                             LO_SHIPPRIOTITY   INTEGER,
                             LO_QUANTITY       BIGINT,
                             LO_EXTENDEDPRICE  BIGINT,
                             LO_ORDTOTALPRICE  BIGINT,
                             LO_DISCOUNT       BIGINT,
                              LO_REVENUE        BIGINT,
                              LO_SUPPLYCOST     BIGINT,
                              LO_TAX            BIGINT,
                              LO_COMMITDATE     INTEGER NOT NULL,
                              LO_SHIPMODE       VARCHAR(10) NOT NULL);
