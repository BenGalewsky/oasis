-- TODO: Add Pig content here
IMPORT 'BusinessLicenseReader.pig';
IMPORT 'CensusTractReader.pig';
IMPORT 'CommunityAreaReader.pig';


tracts = CensusTractReader('data/censusTracts.csv');
businesses  = BusinessLicenseReader('data/businessLicense.csv', 'data/Grocery_Stores_-_2013.csv');
--businesses  = BusinessLicenseReader('data/testBusinesses/*', 'data/Grocery_Stores_-_2013.csv');

STORE businesses INTO 'output/businesses' USING PigStorage(',');


study = CROSS businesses, tracts;
businessTractCross = foreach study GENERATE 
	TRACT, 
	POP10, 
	LICENSE_DESCRIPTION, 
	com.stagrp.bigData.gis.DistanceLatLong(INTPTLAT,INTPTLONG, LATITUDE, LONGITUDE, 'M' ) AS distance:double,
	
	LEGAL_NAME,
	DOING_BUSINESS_AS_NAME,
	ADDRESS,
	CITY,
	businesses::macro_BusinessLicenseReader_origLicenses_0::STATE,
	ZIP_CODE,
	WARD,
	PRECINCT,
	POLICE_DISTRICT,
	LICENSE_CODE,
	LICENSE_NUMBER,
	APPLICATION_TYPE,
	APPLICATION_CREATED_DATE,
	APPLICATION_REQUIREMENTS_COMPLETE,
	PAYMENT_DATE,
	CONDITIONAL_APPROVAL,
	LICENSE_TERM_START_DATE,
	LICENSE_TERM_EXPIRATION_DATE,
	LICENSE_APPROVED_FOR_ISSUANCE,
	DATE_ISSUED,
	LICENSE_STATUS,
	LICENSE_STATUS_CHANGE_DATE,
	SSA,
	LATITUDE,
	LONGITUDE;
	
	

-- Generate interesting slices
oneMile =  FILTER businessTractCross BY $3 < 1.0;
threeMile =  FILTER businessTractCross BY $3 < 3.0;

tractGroups = GROUP oneMile BY (TRACT, LICENSE_DESCRIPTION);
rslt = FOREACH tractGroups GENERATE FLATTEN($0), COUNT($1) AS bizCount;

tractsAtRisk = FILTER rslt BY bizCount == 1;

criticalBiz = JOIN tractsAtRisk BY (TRACT, LICENSE_DESCRIPTION), oneMile BY (TRACT, LICENSE_DESCRIPTION);
criticalBiz2 = JOIN criticalBiz BY (LICENSE_NUMBER), oneMile BY (LICENSE_NUMBER);

impactedPopulationGroups = GROUP criticalBiz2 BY (
						tractsAtRisk::group::businesses::LICENSE_DESCRIPTION,
						oneMile::businesses::macro_BusinessLicenseReader_origLicenses_0::LEGAL_NAME,
						oneMile::businesses::macro_BusinessLicenseReader_origLicenses_0::DOING_BUSINESS_AS_NAME,
						oneMile::businesses::macro_BusinessLicenseReader_origLicenses_0::ADDRESS,
						oneMile::businesses::macro_BusinessLicenseReader_origLicenses_0::STATE,
						oneMile::businesses::macro_BusinessLicenseReader_origLicenses_0::ZIP_CODE,
						oneMile::businesses::macro_BusinessLicenseReader_origLicenses_0::LATITUDE,
						oneMile::businesses::macro_BusinessLicenseReader_origLicenses_0::LONGITUDE
												
);

impactedPopulationCt = FOREACH impactedPopulationGroups GENERATE FLATTEN($0), SUM($1.oneMile::tracts::POP10) AS totalPop10;

STORE oneMile INTO 'output/oneMile' USING PigStorage(',');
STORE tractsAtRisk INTO 'output/tractsAtRisk' USING PigStorage(',');
--STORE criticalBiz INTO 'output/criticalBiz' USING PigStorage(',');
--STORE criticalBiz2 INTO 'output/criticalBiz2' USING PigStorage(',');
--STORE impactedPopulationGroups INTO 'output/impactedPopulationGroups' USING PigStorage(',');
STORE impactedPopulationCt INTO 'output/impactedPopulationCt' USING PigStorage(',');


