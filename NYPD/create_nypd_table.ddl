CREATE TABLE NYPD_COLL (
DATE					DATE NOT NULL,
TIME					TIME NOT NULL,
BOROUGH					VARCHAR(65),
ZIP_CODE				VARCHAR(10),
LATITUDE				DECIMAL(31,10),
LONGITUDE				DECIMAL(31,10),
LOCATION				VARCHAR(65),
ON_STREET_NAME			VARCHAR(65),
CROSS_STREET_NAME		VARCHAR(65),
OFF_STREET_NAME			VARCHAR(65),
PERSONS_INJURED			INT,
PERSONS_KILLED			INT,
PEDESTRIANS_INJURED		INT,
PEDESTRIANS_KILLED		INT,
CYCLIST_INJURED			INT,
CYCLIST_KILLED			INT,
MOTORIST_INJURED		INT,
MOTORIST_KILLED			INT,
CONTRIBUTING_FACTOR_1	VARCHAR(65),
CONTRIBUTING_FACTOR_2	VARCHAR(65),
CONTRIBUTING_FACTOR_3	VARCHAR(65),
CONTRIBUTING_FACTOR_4	VARCHAR(65),
CONTRIBUTING_FACTOR_5	VARCHAR(65),
UNIQUE_KEY				INT NOT NULL,
VEHICLE_TYPE_CODE_1		VARCHAR(65),
VEHICLE_TYPE_CODE_2		VARCHAR(65),
VEHICLE_TYPE_CODE_3		VARCHAR(65),
VEHICLE_TYPE_CODE_4		VARCHAR(65),
VEHICLE_TYPE_CODE_5		VARCHAR(65)
);
