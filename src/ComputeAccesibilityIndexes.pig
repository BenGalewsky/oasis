-- TODO: Add Pig content here
IMPORT 'BusinessLicenseReader.pig';
IMPORT 'CensusTractReader.pig';
IMPORT 'CommunityAreaReader.pig';


tracts = CensusTractReader('data/censusTracts.csv');
businesses  = BusinessLicenseReader('data/businessLicense.csv', 'data/Grocery_Stores_-_2013.csv');

STORE businesses INTO 'output/businesses' USING PigStorage(',');


study = CROSS businesses, tracts;
rslt = foreach study GENERATE 
	TRACT, 
	POP10, 
	LICENSE_DESCRIPTION, 
	com.stagrp.bigData.gis.DistanceLatLong(INTPTLAT,INTPTLONG, LATITUDE, LONGITUDE, 'M' ) AS distance:double;

rsltWithAccess = FOREACH rslt GENERATE
		TRACT, 
	POP10, 
	LICENSE_DESCRIPTION, 
	distance,
	1.0 / distance AS accessibility:double,
	1.0 / POW(distance,2) AS accessibility2:double;
	
	
tractGroups = GROUP rsltWithAccess BY (TRACT, LICENSE_DESCRIPTION, POP10);
access = FOREACH tractGroups GENERATE FLATTEN($0), 
	SUM($1.distance) AS total, 
	SUM($1.accessibility) AS accessibility, 
	SUM($1.accessibility2) AS accessibility2; 
	
STORE access INTO 'output/tracts' USING PigStorage(',');

--tractMap = CommunityAreaReader(
--				'data/Tract_to_Community_Area_Equivalency_File.csv', 
--				'data/CommunityAreas.csv');
--			
--				
--accessWithCommunityAreas = JOIN access BY TRACT, tractMap BY TRACT_CODE;				
			
--communityAreaGroups = GROUP accessWithCommunityAreas BY (COMAREA_ID, COMMUNITY, LICENSE_DESCRIPTION);
--
--communityAccess = FOREACH communityAreaGroups GENERATE FLATTEN($0), 
--	AVG($1.accessibility), 
--	MIN($1.accessibility), 
--	MAX($1.accessibility), 
--	AVG($1.accessibility2),
--	MIN($1.accessibility2), 
--	MAX($1.accessibility2); 
--	
--	
--STORE communityAccess INTO 'output/access' USING PigStorage(',');
--STORE tractGroups INTO 'output/groups' USING PigStorage(',');
--STORE tractMap INTO 'output/maps' USING PigStorage(',');
--
--

