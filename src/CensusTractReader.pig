

DEFINE CensusTractReader(datafile) RETURNS tracts {
	tractsWithHeader = load '$datafile' USING PigStorage(',') as (
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
	$tracts = FILTER tractsWithHeader BY STATE != 'STATE' AND POP10 > 0;

};
