import mysql.connector
from datetime import datetime
import concepts_file_parser

def get_current_vocabularies(cnx):
    vocabularies = {}
    cursor = cnx.cursor()
    query = ("SELECT id, ref FROM vocabularies")
    cursor.execute(query)
    for (id, ref) in cursor:
        if ref not in vocabularies:
            vocabularies[ref] = id
    cursor.close()
    return vocabularies

def get_current_concepts(cnx):
    concepts = {}
    cursor = cnx.cursor()
    query = ("SELECT id, CONCEPT_ID, VOCABULARY_ID, CODE FROM concepts")
    cursor.execute(query)
    for (id,CONCEPT_ID, VOCABULARY_ID, CODE) in cursor:
        concepts[CONCEPT_ID+ "-" +VOCABULARY_ID+ "-" + CODE] = id
    cursor.close()
    return concepts

def add_concepts(concepts, cnx):
    sql = ("""INSERT INTO concepts(PXORDX, OLDPXORDX, CODETYPE, CONCEPT_CLASS_ID, CONCEPT_ID, VOCABULARY_ID, DOMAIN_ID, TRACK, STANDARD_CONCEPT, CODE, CODEWITHPERIODS, CODESCHEME, LONG_DESC, SHORT_DESC, CODE_STATUS, CODE_CHANGE, CODE_CHANGE_YEAR, CODE_PLANNED_TYPE, CODE_BILLING_STATUS, CODE_CMS_CLAIM_STATUS, SEX_CD, ANAT_OR_COND, POA_CODE_STATUS, POA_CODE_CHANGE, POA_CODE_CHANGE_YEAR, VALID_START_DATE, VALID_END_DATE, INVALID_REASON, CREATE_DT)
              VALUES(%s, %s, %s, %s, %s, %s , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")
    index = concepts_file_parser.getIndex()
    valuesList = list()
    for i in index.keys():
        valuesList.append(concepts[i].strip())
    values = tuple(valuesList)
    cursor = cnx.cursor()
    cursor.execute(sql, values)
    cnx.commit()
    return cursor.lastrowid

def add_vocabulary(vocabulary, cnx):
    sql = ("""INSERT INTO vocabularies(ref, name, url, description, status, version)
              VALUES(%s, %s, %s, %s, %s, %s)""")
    values = (
        vocabulary['ref'].strip(),
        vocabulary['name'].strip(),
        vocabulary['url'].strip(),
        vocabulary['description'].strip(),
        vocabulary['status'].strip(),
        vocabulary['version'].strip(),
    )
    cursor = cnx.cursor()
    cursor.execute(sql, values)
    cnx.commit()
    return cursor.lastrowid

def update_task_status(status, uuid, cnx):
    now = datetime.now()
    sql = ("""UPDATE tasks SET status = %s, last_update_date = %s WHERE uuid = %s""")
    values = (
        status,
        now.strftime("%Y-%m-%d %H:%M:%S"),
        uuid,
    )
    cursor = cnx.cursor()
    cursor.execute(sql, values)
    cnx.commit()