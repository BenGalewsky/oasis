-- TODO: Add Pig content here
IMPORT 'BusinessLicenseReader.pig';
IMPORT 'CensusTractReader.pig';
IMPORT 'CommunityAreaReader.pig';
IMPORT 'CountBusinessesInRange.pig';

tracts = CensusTractReader('data/censusTracts.csv');
businesses  = BusinessLicenseReader('data/businessLicense.csv', 'data/Grocery_Stores_-_2013.csv');
--businesses  = BusinessLicenseReader('data/testBusinesses/*', 'data/Grocery_Stores_-_2013.csv');

STORE businesses INTO 'output/businesses' USING org.apache.pig.piggybank.storage.CSVExcelStorage;

study = CROSS businesses, tracts;
rslt = foreach study GENERATE 
	YEAR,
	TRACT, 
	POP10, 
	LICENSE_DESCRIPTION, 
	com.stagrp.bigData.gis.DistanceLatLong(INTPTLAT,INTPTLONG, LATITUDE, LONGITUDE, 'M' ) AS distance:double;

-- Compute the number of businesses of a type within one mile, two miles, and three miles of each tract
oneMile = CountBusinessesInRange(rslt, 1.0);
twoMile = CountBusinessesInRange(rslt, 2.0);
threeMile = CountBusinessesInRange(rslt, 3.0);

ranges3And2 = JOIN 
			threeMile BY(YEAR, TRACT, LICENSE_DESCRIPTION) LEFT OUTER, 
			twoMile BY (YEAR, TRACT, LICENSE_DESCRIPTION);
			
			
ranges123 = JOIN 
			ranges3And2 BY(	
							threeMile::group::businesses::YEAR, 
							threeMile::group::tracts::TRACT, 
							threeMile::group::businesses::LICENSE_DESCRIPTION
			) LEFT OUTER,
			oneMile BY (YEAR, TRACT, LICENSE_DESCRIPTION);
			
			
ranges = FOREACH ranges123 GENERATE
		ranges3And2::threeMile::group::businesses::YEAR as rangeYear,
		ranges3And2::threeMile::group::tracts::TRACT as rangeTract,
		ranges3And2::threeMile::group::businesses::LICENSE_DESCRIPTION as rangeLicenseDescription,
		ranges3And2::threeMile::businesses as threeMileCount,
		ranges3And2::twoMile::businesses as twoMileCount,
		oneMile::businesses as oneMileCount;
		
STORE ranges INTO 'output/ranges' USING org.apache.pig.piggybank.storage.CSVExcelStorage;

-- Compute accesiblity index for each tract to each business
rsltWithAccess = FOREACH rslt GENERATE
	YEAR,
	TRACT, 
	POP10, 
	LICENSE_DESCRIPTION, 
	distance,
	1.0 / distance AS accessibility:double,
	1.0 / POW(distance,2) AS accessibility2:double;
	
	
tractGroups = GROUP rsltWithAccess BY (YEAR, TRACT, LICENSE_DESCRIPTION, POP10);
access = FOREACH tractGroups GENERATE FLATTEN($0), 
	SUM($1.distance) AS total, 
	SUM($1.accessibility) AS accessibility, 
	SUM($1.accessibility2) AS accessibility2; 

accessWithBusiness = JOIN 
				access BY (YEAR, TRACT, LICENSE_DESCRIPTION) LEFT OUTER,
				ranges BY (rangeYear, rangeTract, rangeLicenseDescription);
	
STORE accessWithBusiness INTO 'output/tracts' USING org.apache.pig.piggybank.storage.CSVExcelStorage;

tractMap = CommunityAreaReader(
				'data/Tract_to_Community_Area_Equivalency_File.csv', 
				'data/CommunityAreas.csv');
			
				
accessWithCommunityAreas = JOIN accessWithBusiness BY TRACT, tractMap BY TRACT_CODE;				
			
communityAreaGroups = GROUP accessWithCommunityAreas BY (YEAR, COMAREA_ID, COMMUNITY, LICENSE_DESCRIPTION);

communityAccess = FOREACH communityAreaGroups GENERATE FLATTEN($0), 
	AVG($1.accessibility), 
	MIN($1.accessibility), 
	MAX($1.accessibility), 
	AVG($1.accessibility2),
	MIN($1.accessibility2), 
	MAX($1.accessibility2); 
	
	
STORE communityAccess INTO 'output/access' USING org.apache.pig.piggybank.storage.CSVExcelStorage;
STORE tractGroups INTO 'output/groups' USING org.apache.pig.piggybank.storage.CSVExcelStorage;
STORE tractMap INTO 'output/maps' USING org.apache.pig.piggybank.storage.CSVExcelStorage;




