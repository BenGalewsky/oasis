

DEFINE CommunityAreaReader(tractMapFile, communityAreaFile) RETURNS tractsMapWithAreas {
	tractMapWithHeader = load '$tractMapFile' USING PigStorage(',') as (
		STUSAB:chararray,
		SUMLEV:int,
		COUNTY:int,
		COUSUB:int,
		PLACE:int,
		GEOID2:int,
		CHGOCA:int,
		NAME:chararray,
		TRACT_CODE:chararray
	);
		

	-- Remove Header
	tractsMap = FILTER tractMapWithHeader BY STUSAB != 'STUSAB';
	
	communityAreasWithHeader = load '$communityAreaFile' USING PigStorage(',') as (
		PERIMETER:double,	
		AREA:double,
		COMAREA:chararray,	
		COMAREA_ID:int,
		AREA_NUMBER:int,	
		COMMUNITY:chararray,	
		AREA_NUM_1:int,	
		SHAPE_AREA:double,	
		SHAPE_LEN:double	
	);

	-- Remove Header
	communityAreas = FILTER communityAreasWithHeader BY COMMUNITY != 'STUSAB';
	$tractsMapWithAreas = JOIN tractsMap BY CHGOCA, communityAreas BY AREA_NUMBER;				
};
