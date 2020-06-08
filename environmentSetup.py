#!/usr/local/bin/python3.6
# -*- coding: utf-8 -*-
"""
Created on Wed May 27 20:49:49 2020

@author: zahirSheikh

This framework accept three parameters (i.e. Source Schema, Target Schema and 
metadata directory path --> sourceSchema_TargetSchema_datetimestamp) in order
to sync up database schema by connecting Stage DB with Hardcoded connection param.
This script is divided into five logical section i.e.
1. Setting the Oracle Session Parameters --> This requires to avoid additional
information getting pull up in DDL i.e. Storage Type, locations etc.
2. Table Sync up section --> This section is divided furthermore into two subsections
    A. Column Mismatch --> It compares the same table in two schemas and find 
       out the column anomaly for example table zahir has 12 columns in source
       schema where as in target schema it has less/more (i.e. 11/14) columns 
       then it would drop this table from the target schema so that it would be
       picked up in missing table section.
    B. Pushing Missing Tables --> This code consider tables are missing in target
       schema by comparing with source schema and generates the DDL for such tables.
       Once metadata DDL gets generated it writes back to csv file in metadata 
       directory by name 'tableMissTgtDF.csv' so that it can be used for audit 
       purpose or reference.
3. Index Sync up section --> This section is divided moreover into two subsections
    A. Column Mismatch --> It compares indexes in two schemas and find out the 
       column anomaly for example index zahirIDX has 12 columns in source schema
       where as in target schema it has less/more (i.e. 11/14) columns then it 
       would drop this index from the target schema so that it would be picked 
       up in missing index section.
    B. Pushing Missing Indexes --> This code consider indexes are missing in 
       target schema by comparing with source schema and generates the DDL for 
       such indexes. Once metadata DDL gets generated it writes back to csv file
       in metadata directory by name 'indexMissTgtDF.csv' so that it can be used
       for audit purpose or reference.       
4. Procedure/Function Sync up section --> This section is divided additionally 
   into two subsections
    A. Text Mismatch within line of SP --> It simply count the number of lines 
       of PL/SQL block in two schemas and drop it from the target schema so that
       it would be picked up in missing Procedure/Function section as logic needs
       to revisit to find the textual difference within a line. Based on new logic
       code needs to be rewritten.
    B. Pushing Missing PL/SQL Blocks --> This code consider all the PL/SQL blocks
       are missing in target schema as well as count mismatch for the number of
       lines by comparing with source schema and generates the PL/SQL Script for
       such SP. Once metadata PL/SQL block gets generated, it writes back to csv
       file in metadata directory by name 'storeprocMissTgtDF.csv' so that it can
       be used for audit purpose or reference.
5. Grant section --> This section is divided also into two subsections
    A. GRANTOR --> This provide privileges on the target objects to USER/SCHEMA 
       so that DML operation can be executed on it.
    B. GRANTEE --> This provide privileges on the objects of other schemas to 
       target schema so that DML operation can be executed by it.
       
"""

import pandas as pd
import cx_Oracle
import time
import sys
#import json 
#import re


start = time.time()
connFlag = 0
try:
    
    conn= cx_Oracle.connect('CLNADMIN/CLNADMIN_STG@ORIONSTG')
    cur = conn.cursor()
    print("Connection successfully created between Stage App and DB Server")
    connFlag = 1
except cx_Oracle.DatabaseError as exc:
    err, = exc.args
    print("Failed to connect to Stage Database")
    print("Oracle-Error-Code:", err.code)
    print("Oracle-Error-Message:", err.message)
    #exit (1)

# sourceSchema = 'MIGRATION_8'
# targetSchema = 'MIGRATION_6'
# metaDataPath = 'C:\Zahir\Orion\00 EnviornmentSetup\05262020'
# metaDataPath = '/paas/projects/envSetup/metaData'
sourceSchema = str(sys.argv[1]).upper()
targetSchema = str(sys.argv[2]).upper()
metaDataPath = str(sys.argv[3])
envFileExt = sourceSchema[-2:]+"_To"+targetSchema[-2:]+".csv"
print('source schema: {} and target schema: {}'.format(sourceSchema,targetSchema))
print('Meta Data Path to store DDL commands: {}'.format(metaDataPath))

    
def objectCompareExecution ():   
    
    exceptionDF = pd.DataFrame(columns=['OBJECT_TYPE', 'OBJECT_NAME', 'OBJECT_CODE','ERROR_CODE','ERROR_MESSAGE'])
    # Set Oracle Session Parameters
    sessionSetQry = """BEGIN

                        DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
                        DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
                        DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
                        DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);

                    END;"""
    try:
        cur.execute(sessionSetQry)
    except cx_Oracle.DatabaseError as exc:
        err, = exc.args
        exceptionDF = exceptionDF.append([{'OBJECT_TYPE':"PL/SQL BLOCK", 'OBJECT_NAME':"SETUP Env.", 'OBJECT_CODE':"SESSION PARAMETER",'ERROR_CODE':str(err.code),'ERROR_MESSAGE':str(err.message)}],ignore_index=True)
        
    print("Session Parameter Setup is finished")
    
    # Table Section    
    Query =  """ SELECT 'TABLE' AS OBJECT_TYPE, TABLE_NAME, C1, C2
                FROM (
                        SELECT TABLE_NAME, MAX(DECODE(OWNER, '"""+sourceSchema+"""', COLUMN_ID, NULL)) C1, 
                        MAX(DECODE(OWNER, '"""+targetSchema+"""', COLUMN_ID, NULL)) C2
                        FROM DBA_TAB_COLUMNS
                        WHERE OWNER IN ('"""+sourceSchema+"""','"""+targetSchema+"""') AND TABLE_NAME NOT LIKE 'BIN%'
                        GROUP BY TABLE_NAME 
                    ) A
                WHERE A.C1 <> A.C2 AND TABLE_NAME NOT LIKE 'TMP%'"""
    tableDropTgtDF = pd.read_sql(Query, con = conn)
    tableDropTgtDF.to_csv(metaDataPath+'/tableDropTgtDF'+envFileExt, sep="|", header=True)
    tableDropTgtDF = tableDropTgtDF.astype(object).where(pd.notnull(tableDropTgtDF), None)
    for x in tableDropTgtDF['TABLE_NAME'].values:
        try:
            cur.execute('''DROP TABLE '''+targetSchema+'''.'''+x)        
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            exceptionDF = exceptionDF.append([{'OBJECT_TYPE':"DROP TABLE", 'OBJECT_NAME':x, 'OBJECT_CODE':"DROP TABLE "+targetSchema+"."+x,'ERROR_CODE':str(err.code),'ERROR_MESSAGE':str(err.message)}],ignore_index=True)
     
    conn.commit()
    print("Table Deletion is finished")
    Query =  """ SELECT 'TABLE' AS OBJECT_TYPE, TABLE_NAME, 
    REPLACE(REPLACE(DBMS_METADATA.GET_DDL('TABLE', TABLE_NAME, '"""+sourceSchema+"""'),'"""+sourceSchema+"""','"""+targetSchema+"""'),';','') AS CREATE_TABLE_SQL, C1, C2
    FROM (
            SELECT TABLE_NAME, MAX(DECODE(OWNER, '"""+sourceSchema+"""', COLUMN_ID, NULL)) C1, 
            MAX(DECODE(OWNER, '"""+targetSchema+"""', COLUMN_ID, NULL)) C2
            FROM DBA_TAB_COLUMNS
            WHERE OWNER IN ('"""+sourceSchema+"""','"""+targetSchema+"""') AND TABLE_NAME NOT LIKE 'BIN%'
            GROUP BY TABLE_NAME 
        ) A
    WHERE A.C2 IS NULL AND TABLE_NAME NOT LIKE 'TMP%' """
    tableMissTgtDF = pd.read_sql(Query, con = conn)
    tableMissTgtDF.to_csv(metaDataPath+'/tableMissTgtDF'+envFileExt, sep="|", header=True)
    tableMissTgtDF = tableMissTgtDF.astype(object).where(pd.notnull(tableMissTgtDF), None)
    tableMissTgtDF = tableMissTgtDF.replace(to_replace = ";",value ="")
    # for x in tableMissTgtDF['CREATE_TABLE_SQL'].values:
    #     cur.execute(str(x))  
    
    for i in tableMissTgtDF.index : 
        try:
            cur.execute(str(tableMissTgtDF['CREATE_TABLE_SQL'][i]))        
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            exceptionDF = exceptionDF.append([{'OBJECT_TYPE':"CREATE TABLE", 'OBJECT_NAME':str(tableMissTgtDF['TABLE_NAME'][i]), 'OBJECT_CODE':str(tableMissTgtDF['CREATE_TABLE_SQL'][i]),'ERROR_CODE':str(err.code),'ERROR_MESSAGE':str(err.message)}],ignore_index=True)
     
    conn.commit()
    print("Table Creation is finished")

    # Table Index Section    
    Query =  """ SELECT 'INDEX' AS OBJECT_TYPE, TABLE_NAME, INDEX_NAME, C1, C2
                FROM (
                        SELECT TABLE_NAME, INDEX_NAME, MAX(DECODE(INDEX_OWNER, '"""+sourceSchema+"""', COLUMN_POSITION, NULL)) C1, 
                        MAX(DECODE(INDEX_OWNER, '"""+targetSchema+"""', COLUMN_POSITION, NULL)) C2
                        FROM DBA_IND_COLUMNS 
                        WHERE INDEX_OWNER IN ('"""+sourceSchema+"""','"""+targetSchema+"""') AND TABLE_NAME NOT LIKE 'BIN%'
                        GROUP BY TABLE_NAME, INDEX_NAME 
                    ) A
                WHERE A.C1 <> A.C2 AND TABLE_NAME NOT LIKE 'TMP%'"""
    indexDropDF = pd.read_sql(Query, con = conn)
    indexDropDF.to_csv(metaDataPath+'/indexDropDF'+envFileExt, sep="|", header=True)
    indexDropDF = indexDropDF.astype(object).where(pd.notnull(indexDropDF), None)
    for x in indexDropDF['INDEX_NAME'].values:
        try:
            cur.execute('''DROP INDEX '''+targetSchema+'''.'''+x)        
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            exceptionDF = exceptionDF.append([{'OBJECT_TYPE':"DROP INDEX", 'OBJECT_NAME':x, 'OBJECT_CODE':"DROP INDEX "+targetSchema+"."+x,'ERROR_CODE':str(err.code),'ERROR_MESSAGE':str(err.message)}],ignore_index=True)
     
    conn.commit()
    print("Index Deletion is finished")
    Query =  """ SELECT 'INDEX' AS OBJECT_TYPE, TABLE_NAME, INDEX_NAME, 
    REPLACE(REPLACE(DBMS_METADATA.GET_DDL('INDEX', INDEX_NAME, '"""+sourceSchema+"""'),'"""+sourceSchema+"""','"""+targetSchema+"""'),';','') AS CREATE_INDEX_SQL, 
    C1, C2
    FROM (
            SELECT TABLE_NAME, INDEX_NAME, MAX(DECODE(INDEX_OWNER, '"""+sourceSchema+"""', COLUMN_POSITION, NULL)) C1, 
            MAX(DECODE(INDEX_OWNER, '"""+targetSchema+"""', COLUMN_POSITION, NULL)) C2
            FROM DBA_IND_COLUMNS 
            WHERE INDEX_OWNER IN ('"""+sourceSchema+"""','"""+targetSchema+"""') AND TABLE_NAME NOT LIKE 'BIN%'
            GROUP BY TABLE_NAME, INDEX_NAME 
        ) A
    WHERE A.C2 IS NULL AND TABLE_NAME NOT LIKE 'TMP%' """
    indexMissTgtDF = pd.read_sql(Query, con = conn)
    indexMissTgtDF.to_csv(metaDataPath+'/indexMissTgtDF'+envFileExt, sep="|", header=True)
    indexMissTgtDF = indexMissTgtDF.astype(object).where(pd.notnull(indexMissTgtDF), None)
    indexMissTgtDF = indexMissTgtDF.replace(to_replace = ";",value ="")
    # for x in indexMissTgtDF['CREATE_INDEX_SQL'].values:
    #     cur.execute(str(x))
    
    for i in indexMissTgtDF.index : 
        try:
            cur.execute(str(indexMissTgtDF['CREATE_INDEX_SQL'][i]))        
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            exceptionDF = exceptionDF.append([{'OBJECT_TYPE':"CREATE INDEX", 'OBJECT_NAME':str(indexMissTgtDF['INDEX_NAME'][i]), 'OBJECT_CODE':str(indexMissTgtDF['CREATE_INDEX_SQL'][i]),'ERROR_CODE':str(err.code),'ERROR_MESSAGE':str(err.message)}],ignore_index=True)
    
    conn.commit()
    print("Index Creation is finished") 

    # Procedure and Function Section    
    Query =  """ SELECT DISTINCT A.TYPE, A.NAME
                    FROM (SELECT NAME, TYPE,
                    MAX(DECODE(OWNER, '"""+sourceSchema+"""', LINE, NULL)) C1,
                    MAX(DECODE(OWNER, '"""+targetSchema+"""', LINE, NULL)) C2
                    FROM DBA_SOURCE
                    WHERE OWNER IN ('"""+sourceSchema+"""', '"""+targetSchema+"""') AND TYPE IN ('PROCEDURE','FUNCTION')
                    GROUP BY NAME, TYPE) A
                    WHERE A.C1 = A.C2 """
    storeprocDropDF = pd.read_sql(Query, con = conn)
    storeprocDropDF.to_csv(metaDataPath+'/storeprocDropDF'+envFileExt, sep="|", header=True)
    storeprocDropDF = storeprocDropDF.astype(object).where(pd.notnull(storeprocDropDF), None)
    storeprocDropDF = storeprocDropDF.replace(to_replace = ";",value ="")
    # for x in storeprocDropDF.values:
    #     cur.execute('''DROP '''+x[0]+''' '''+targetSchema+'''.''',x[1])
    
    for x in storeprocDropDF.values:    
        try:
            cur.execute('''DROP '''+x[0]+''' '''+targetSchema+'''.''',x[1])        
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            exceptionDF = exceptionDF.append([{'OBJECT_TYPE':"DROP "+str(x[0]), 'OBJECT_NAME':str(x[1]), 'OBJECT_CODE':"DROP "+str(x[0])+" "+targetSchema+"."+str(x[1]),'ERROR_CODE':str(err.code),'ERROR_MESSAGE':str(err.message)}],ignore_index=True)
     
    conn.commit()
    print("Procedure/Function Deletion is finished")
    Query = """SELECT NAME, TYPE,
        REPLACE(REPLACE(DBMS_METADATA.GET_DDL(TYPE,NAME,'"""+sourceSchema+"""'),'"""+sourceSchema+"""','"""+targetSchema+"""'),'"','') AS "SCRIPT_BODY",
        C1, C2
        FROM (SELECT NAME, TYPE,
        MAX(DECODE(OWNER, '"""+sourceSchema+"""', LINE, NULL)) C1,
        MAX(DECODE(OWNER, '"""+targetSchema+"""', LINE, NULL)) C2
        FROM DBA_SOURCE
        WHERE OWNER IN ('"""+sourceSchema+"""', '"""+targetSchema+"""') AND TYPE IN ('PROCEDURE','FUNCTION')
        GROUP BY NAME, TYPE) A
        WHERE A.C1 <> A.C2
        OR A.C2 IS NULL """
    storeprocMissTgtDF = pd.read_sql(Query, con = conn)
    storeprocMissTgtDF.to_csv(metaDataPath+'/storeprocMissTgtDF'+envFileExt, sep="|", header=True)
    storeprocMissTgtDF = storeprocMissTgtDF.astype(object).where(pd.notnull(storeprocMissTgtDF), None)
    storeprocMissTgtDF = storeprocMissTgtDF.replace(to_replace = '"',value ='')
    # for x in storeprocMissTgtDF['SCRIPT_BODY'].values:
    #     cur.execute(str(x))
    
    for i in storeprocMissTgtDF.index : 
        try:
            cur.execute(str(storeprocMissTgtDF['SCRIPT_BODY'][i]))        
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            exceptionDF = exceptionDF.append([{'OBJECT_TYPE':"CREATE "+str(storeprocMissTgtDF['TYPE'][i]), 'OBJECT_NAME':str(storeprocMissTgtDF['NAME'][i]), 'OBJECT_CODE':str(storeprocMissTgtDF['SCRIPT_BODY'][i]),'ERROR_CODE':str(err.code),'ERROR_MESSAGE':str(err.message)}],ignore_index=True)
    
    conn.commit()
    print("Procedure/Function Creation is finished") 
    # Grant Section
    Query ="""SELECT 'Grant ' ||PRIVILEGE  ||' on """+targetSchema+""".'  ||TABLE_NAME  ||' to '||GRANTEE AS "GRANT_SQL"
            FROM dba_tab_privs
            WHERE GRANTOR ='"""+sourceSchema+"""'
            AND OWNER ='"""+sourceSchema+"""'
            AND TABLE_NAME NOT LIKE 'BIN%' --and GRANTEE = 'CLNADMIN' 
            and (TABLE_NAME  NOT LIKE '%TMP%' OR TABLE_NAME  NOT LIKE '%TEST%') """
    grantorTgtDF = pd.read_sql(Query, con = conn)
    grantorTgtDF.to_csv(metaDataPath+'/grantorTgtDF'+envFileExt, sep="|", header=True)
    grantorTgtDF = grantorTgtDF.astype(object).where(pd.notnull(grantorTgtDF), None)
    grantorTgtDF = grantorTgtDF.replace(to_replace = ";",value ="")
    for x in grantorTgtDF.values:
        try:
            cur.execute(str(x[0]))
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            exceptionDF = exceptionDF.append([{'OBJECT_TYPE':"GRANTOR", 'OBJECT_NAME':str(x[0]), 'OBJECT_CODE':str(x[0]),'ERROR_CODE':str(err.code),'ERROR_MESSAGE':str(err.message)}],ignore_index=True)
     
    conn.commit()
    print("Grantor Priviledge Provition is finished") 
    Query ="""SELECT 'Grant ' ||PRIVILEGE  ||' on ' ||OWNER  ||'.' ||TABLE_NAME  ||' to """+targetSchema+"""' AS "GRANT_SQL"
            FROM dba_tab_privs
            WHERE GRANTEE='"""+sourceSchema+"""'
            AND TABLE_NAME NOT LIKE 'BIN%' --and GRANTEE = 'CLNADMIN' 
            and (TABLE_NAME  NOT LIKE '%TMP%' OR TABLE_NAME  NOT LIKE '%TEST%') """
    granteeTgtDF = pd.read_sql(Query, con = conn)
    granteeTgtDF.to_csv(metaDataPath+'/granteeTgtDF'+envFileExt, sep="|", header=True)
    granteeTgtDF = granteeTgtDF.astype(object).where(pd.notnull(granteeTgtDF), None)
    granteeTgtDF = granteeTgtDF.replace(to_replace = ";",value ="")
    for x in granteeTgtDF.values:
        try:
            cur.execute(str(x[0]))
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            exceptionDF = exceptionDF.append([{'OBJECT_TYPE':"GRANTEE", 'OBJECT_NAME':str(x[0]), 'OBJECT_CODE':str(x[0]),'ERROR_CODE':str(err.code),'ERROR_MESSAGE':str(err.message)}],ignore_index=True)
     
    conn.commit()
    print("Grantee Priviledge Provition is finished") 
    
    exceptionDF['OBJECT_CODE'].to_csv(metaDataPath+'/exceptionDDLCodeDF'+envFileExt, sep="|", header=True)
    print("Exception File with DDL code creation is finished") 
    exceptionsDF = exceptionDF[['OBJECT_TYPE', 'OBJECT_NAME', 'ERROR_CODE','ERROR_MESSAGE']]
    exceptionsDF.to_csv(metaDataPath+'/exceptionDF'+envFileExt, sep="|", header=True)
    print("Exception File without DDL code creation is finished") 
    
if len(sourceSchema) != 0 and len(targetSchema) != 0 :
    if connFlag == 1:
        objectCompareExecution()
       # Closing cursor as well as DB Connection 
        cur.close()
        conn.close()

elapsed = (time.time() - start)
print("Total Time elapsed #### " + str(elapsed))    