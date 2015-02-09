DEFINE BusinessLicenseReader(filename, groceryFilename) RETURNS licenses{
	
	licensesWithHeader = load '$filename' USING org.apache.pig.piggybank.storage.CSVExcelStorage as (
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

-- Filter out headers, bad data and expired licenses
origLicenses = FILTER licensesWithHeader BY 
	(ID != 'ID') 
	AND (LATITUDE is not null) AND (LONGITUDE is not null)
--	AND LICENSE_STATUS == 'AAI'
--	AND (LICENSE_TERM_EXPIRATION_DATE is not null AND LICENSE_TERM_EXPIRATION_DATE != '') 
--	AND (LICENSE_TERM_START_DATE is not null AND LICENSE_TERM_START_DATE != '')
--	AND  DaysBetween(ToDate(LICENSE_TERM_EXPIRATION_DATE, 'MM/dd/YYYY'), ToDate('02/09/2015', 'MM/dd/YYYY')) > 0 
--	AND  DaysBetween(ToDate(LICENSE_TERM_START_DATE, 'MM/dd/YYYY'), ToDate('02/09/2015', 'MM/dd/YYYY')) < 0;
;

groceriesWithHeader = load '$groceryFilename' USING org.apache.pig.piggybank.storage.CSVExcelStorage as (
	G_STORE_NAME:chararray,	
	G_LICENSE_ID:int,	
	G_ACCOUNT_NUMBER:int,	
	G_SQUARE_FEET:int,
	G_BUFFER_SIZE:chararray,
	G_ADDRESS:chararray,	
	G_ZIP:chararray, 
	G_COMMUNITY_AREA_NAME:chararray,	
	G_COMMUNITY_AREA_WARD:int,
	G_CENSUS_TRACT:chararray,	
	G_CENSUS_BLOCK:chararray,
	G_X_COORDINATE:double,
	G_Y_COORDINATE:double,
	G_LATITUDE:double,	
	G_LONGITUDE:double,	
	G_LOCATION:chararray
);

groceries = FILTER groceriesWithHeader by G_STORE_NAME != 'STORE_NAME';

groceryJoin = JOIN  origLicenses BY LICENSE_NUMBER LEFT OUTER, groceries BY G_LICENSE_ID;

$licenses = FOREACH groceryJoin GENERATE 
	ID,
	LICENSE_ID,
	ACCOUNT_NUMBER,
	SITE_NUMBER,
	LEGAL_NAME,
	DOING_BUSINESS_AS_NAME,
	ADDRESS,
	CITY,
	STATE,
	ZIP_CODE,
	WARD,
	PRECINCT,
	POLICE_DISTRICT,
	LICENSE_CODE,	
	(G_STORE_NAME IS NOT NULL ? 'Grocery' : LICENSE_DESCRIPTION) AS LICENSE_DESCRIPTION,	
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
	LONGITUDE,
	G_SQUARE_FEET;



	
};

