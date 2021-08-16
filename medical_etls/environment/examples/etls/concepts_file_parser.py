import utils

index={'PXORDX': 0, 'OLDPXORDX': 1, 'CODETYPE': 2, 'CONCEPT_CLASS_ID': 3, 'CONCEPT_ID': 4, 'VOCABULARY_ID': 5, 'DOMAIN_ID': 6, 'TRACK': 7,
'STANDARD_CONCEPT': 8, 'CODE': 9, 'CODEWITHPERIODS': 10, 'CODESCHEME': 11, 'LONG_DESC': 12, 'SHORT_DESC': 13, 'CODE_STATUS': 14, 'CODE_CHANGE': 15,
'CODE_CHANGE_YEAR': 16, 'CODE_PLANNED_TYPE': 17, 'CODE_BILLING_STATUS': 18, 'CODE_CMS_CLAIM_STATUS': 19, 'SEX_CD': 20, 'ANAT_OR_COND': 41,
'POA_CODE_STATUS': 50, 'POA_CODE_CHANGE': 51, 'POA_CODE_CHANGE_YEAR': 52, 'VALID_START_DATE': 53, 'VALID_END_DATE': 54, 'INVALID_REASON': 55,
'CREATE_DT': 56}

def getIndex():
	return index

def get_concept(item):
	dic=dict()
	for i in index.keys():
		dic[i]=utils.get_value_or_default(item[index[i]])
	return dic
