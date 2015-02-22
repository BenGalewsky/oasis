-- TODO: Add Pig content here
IMPORT 'BusinessLicenseReader.pig';
IMPORT 'CensusTractReader.pig';
IMPORT 'CommunityAreaReader.pig';


-- Load in the list of census tracts and the Lat/Long centroids
tracts = CensusTractReader('data/censusTracts.csv');

-- Load in the list of business licesness and enrich with the grocery store extract
businesses  = BusinessLicenseReader('data/businessLicense.csv', 'data/Grocery_Stores_-_2013.csv');
--businesses  = BusinessLicenseReader('data/testBusinesses/*', 'data/Grocery_Stores_-_2013.csv');


STORE businesses INTO 'output/businesses' USING org.apache.pig.piggybank.storage.CSVExcelStorage;

-- Create a study where we look at the distance from every census tract to every business 
-- in the city
study = CROSS businesses, tracts;
businessTractCross = foreach study GENERATE 
	YEAR,
	TRACT, 
	POP10, 
	LICENSE_DESCRIPTION, 
	com.stagrp.bigData.gis.DistanceLatLong(INTPTLAT,INTPTLONG, LATITUDE, LONGITUDE, 'M' ) AS distance:double,
	LEGAL_NAME,
	DOING_BUSINESS_AS_NAME,
	ADDRESS,
	CITY,
	macro_BusinessLicenseReader_groceryJoin_0::macro_BusinessLicenseReader_origLicenses_0::STATE,
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
	

-- Compute count of each business type within one mile of every 
-- census tract (by year)
oneMile =  FILTER businessTractCross BY distance < 1.0;
tractGroups = GROUP oneMile BY (YEAR, TRACT, LICENSE_DESCRIPTION);
rslt = FOREACH tractGroups GENERATE FLATTEN($0), COUNT($1) AS bizCount;

-- Now find the census tracts which are at risk of desertification with respect to
-- a business type in any particular year
tractsAtRisk = FILTER rslt BY bizCount == 1;

-- Now lets find the businesses that serve those tracts
criticalBiz = JOIN tractsAtRisk BY (YEAR, TRACT, LICENSE_DESCRIPTION), oneMile BY (YEAR, TRACT, LICENSE_DESCRIPTION);
criticalBiz2 = JOIN criticalBiz BY (tractsAtRisk::group::businesses::YEAR, LICENSE_NUMBER), oneMile BY (YEAR, LICENSE_NUMBER);

-- Now we need to compute the total number of people who depend on each business
impactedPopulationGroups = GROUP criticalBiz2 BY (
						oneMile::businesses::YEAR,
						tractsAtRisk::group::businesses::LICENSE_DESCRIPTION,
						oneMile::businesses::macro_BusinessLicenseReader_groceryJoin_0::macro_BusinessLicenseReader_origLicenses_0::LEGAL_NAME,
						oneMile::businesses::macro_BusinessLicenseReader_groceryJoin_0::macro_BusinessLicenseReader_origLicenses_0::DOING_BUSINESS_AS_NAME,
						oneMile::businesses::macro_BusinessLicenseReader_groceryJoin_0::macro_BusinessLicenseReader_origLicenses_0::ADDRESS,
						oneMile::businesses::macro_BusinessLicenseReader_groceryJoin_0::macro_BusinessLicenseReader_origLicenses_0::STATE,
						oneMile::businesses::macro_BusinessLicenseReader_groceryJoin_0::macro_BusinessLicenseReader_origLicenses_0::ZIP_CODE,
						oneMile::businesses::macro_BusinessLicenseReader_groceryJoin_0::macro_BusinessLicenseReader_origLicenses_0::LATITUDE,
						oneMile::businesses::macro_BusinessLicenseReader_groceryJoin_0::macro_BusinessLicenseReader_origLicenses_0::LONGITUDE
												
);

impactedPopulationCt = FOREACH impactedPopulationGroups GENERATE FLATTEN($0), SUM($1.oneMile::tracts::POP10) AS totalPop10;

STORE oneMile INTO 'output/oneMile' USING org.apache.pig.piggybank.storage.CSVExcelStorage;
STORE tractsAtRisk INTO 'output/tractsAtRisk' USING org.apache.pig.piggybank.storage.CSVExcelStorage;
STORE impactedPopulationCt INTO 'output/impactedPopulationCt' USING org.apache.pig.piggybank.storage.CSVExcelStorage;


