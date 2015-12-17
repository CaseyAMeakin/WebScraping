"""
Wrappers for SQL queries on a sqlite3 database.
"""

import sys
import sqlite3 as lite


def escapeQuotes(string):
    string = re.sub(u"\"",u"\"\"",unicode(string))
    string = re.sub(u"\'",u"\'\'",unicode(string))
    return string


def connectDB(sqlite3_db_file_name):
    con = lite.connect(sqlite3_db_file_name)
    return con


def updateTableRowKeyValueString(con,table,rowid,key,stringValue):
    cur = con.cursor()
    sql_ = u"""update {0} set {1} =  "{2}" where rowid = {3};"""
    sqlcmd = sql_.format(table,key,stringValue,rowid)    
    try:
        results = cur.execute(sqlcmd).fetchall()
    except:
        print 'Error: ' + sqlcmd
        results = None
    con.commit()
    return result


def trySqlcmdFetchall(con,sqlcmd,logfile=None,quiet=True,logging=False):
    cur = con.cursor()
    try: 
        results = cur.execute(sqlcmd).fetchall()
        statCode = 'Success'
    except:
        results = None
        statCode = 'Error'
    logmsg = u"""[{0},{1},{2}]"""
    msg = logmsg.format(datetime.now().isoformat(),sqlcmd,statCode)
    if(logging): logfile.write(msg+"\n")
    if(not quiet): print msg
    return results


def trySqlcmdCommit(con,sqlcmd,logfile=None,quiet=True,logging=False):
    cur = con.cursor()
    try:
        cur.execute(sqlcmd)
        statCode = 'Success'
        con.commit()
        rowid = cur.lastrowid
    except:
        if(not quiet): print 'ERROR: ' + sqlcmd
        statCode = 'Error'
        rowid = 0
    logmsg = u"""[{0},{1},{2}]"""
    msg = logmsg.format(datetime.now().isoformat(),sqlcmd,statCode)
    if(logging): logfile.write(msg+"\n")
    if(not quiet): print msg
    return rowid


