-- TODO: Add Pig content here
--REGISTER myudfs.jar;

tractsWithHeader = load 'data/censusTracts.csv' USING PigStorage(',') as (
STATE:chararray,
GEOID:chararray,
POP10:int,
HU10:int,
ALAND:int,
AWATER:int,
ALAND_SQMI:double,
AWATER_SQMI:double,
INTPTLAT:double,
INTPTLONG:double,
STATEID:double,
COUNTY_ID:int,
TRACT:chararray
);


-- Remove Header
tracts = FILTER tractsWithHeader BY STATE != 'STATE' AND POP10 > 0;


licensesWithHeader = load 'data/businessLicense.csv' USING PigStorage(',') as (
	ID:chararray,
	LICENSE_ID:int,
	ACCOUNT_NUMBER:int,
	SITE_NUMBER:int,
	LEGAL_NAME:chararray,
	DOING_BUSINESS_AS_NAME:chararray,
	ADDRESS:chararray,
	CITY:chararray,
	STATE:chararray,
	ZIP_CODE:int,
	WARD:int,
	PRECINCT:int,
	POLICE_DISTRICT:int,
	LICENSE_CODE:int,
	LICENSE_DESCRIPTION:chararray,
	LICENSE_NUMBER:int,
	APPLICATION_TYPE:chararray,
	APPLICATION_CREATED_DATE:chararray,
	APPLICATION_REQUIREMENTS_COMPLETE:chararray,
	PAYMENT_DATE:chararray,
	CONDITIONAL_APPROVAL:chararray,
	LICENSE_TERM_START_DATE:chararray,
	LICENSE_TERM_EXPIRATION_DATE:chararray,
	LICENSE_APPROVED_FOR_ISSUANCE:chararray,
	DATE_ISSUED:chararray,
	LICENSE_STATUS:chararray,
	LICENSE_STATUS_CHANGE_DATE:chararray,
	SSA:chararray,
	LATITUDE:double,
	LONGITUDE:double,
	LOCATION:chararray
);


licenses = FILTER licensesWithHeader BY (ID != 'ID') AND (LATITUDE is not null) AND (LONGITUDE is not null) ;

study = CROSS licenses, tracts;

rslt = foreach study GENERATE TRACT, POP10, LICENSE_DESCRIPTION, com.stagrp.bigData.gis.DistanceLatLong(INTPTLAT,INTPTLONG, LATITUDE, LONGITUDE, 'M' );

oneMile =  FILTER rslt BY $3 < 1.0;
threeMile =  FILTER rslt BY $3 < 3.0;

tractGroups = GROUP oneMile BY (TRACT, LICENSE_DESCRIPTION);


DUMP tractGroups;

rslt = FOREACH tractGroups GENERATE $0, COUNT($1);
DUMP rslt;
 store tractGroups into 'tmp/foo/tractGroups' using PigStorage(',');
 store rslt into 'tmp/foo/rslt' using PigStorage(',');
 
