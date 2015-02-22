DEFINE CountBusinessesInRange(businessTractCross, range) RETURNS businessesInRange {
	filteredBusinesses = FILTER $businessTractCross BY distance < $range;
	
	tractGroups = GROUP filteredBusinesses BY (YEAR, TRACT, LICENSE_DESCRIPTION);
	$businessesInRange = FOREACH tractGroups GENERATE FLATTEN($0), COUNT($1) AS businesses:int;		
};