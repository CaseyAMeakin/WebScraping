"""
Mostly wrappers for SQL queries on a database to store
scraped Rotten Tomatoes data.
"""

import sys
from datetime import *
from RottenTomatoes import *
import sqlite3 as lite

def escapeQuotes(string):
    string = re.sub("\"","\"\"",string)
    string = re.sub("\'","\'\'",string)
    return string


# Return list of movie data arrays, each like: [Array](year, title) 
def readMovieList(movie_list_file):
    movie_list = open(movie_list_file)
    movies = []
    for line in movie_list:
        year = line.split()[0]
        title = line.split(year + '\t')[1].strip()
        #print year, title
        movies.append((year,title))
    return movies

# Connect to DB, return connection
def connectDB(sqlite3_db_file_name):
    con = lite.connect(sqlite3_db_file_name)
    return con

# Insert Title and Year into sqlite3 table "movie"
def populateTitleYear(con,movieList):
    cur = con.cursor()
    sql_ =  """insert into movies (year, title) values ({0}, "{1}");"""
    for movie in movieList:
        sqlcmd = sql_.format(movie[0],movie[1])
        try:
            result = cur.execute(sqlcmd)
        except:
            print 'ERROR: ' + sqlcmd
    con.commit()


# Update a string column for a given row in a sqlite3 database
def updateTableRowKeyValueString(con,table,rowid,key,stringValue):
    cur = con.cursor()
    sql_ = """update {0} set {1} =  "{2}" where rowid = {3};"""
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
    logmsg = """[{0},{1},{2}]"""
    msg = logmsg.format(datetime.now().isoformat(),sqlcmd,statCode)
    if(logging): logfile.write(msg+"\n")
    if(not quiet): print msg
    return results


def trySqlcmdCommit(con,sqlcmd,logfile=None,quiet=True,logging=False):
    cur = con.cursor()
    try:
        cur.execute(sqlcmd)
        statCode = 'Scucces'
        con.commit()
    except:
        if(not quiet): print 'ERROR: ' + sqlcmd
        statCode = 'Error'
    logmsg = """[{0},{1},{2}]"""
    msg = logmsg.format(datetime.now().isoformat(),sqlcmd,statCode)
    if(logging): logfile.write(msg+"\n")
    if(not quiet): print msg
    return cur.lastrowid


def updateMovieMetaDataRTDB(con,movieid,metaData,logfile=None,logging=False,quiet=True):
    """
    Scrape RT URL for movie meta data.
    Insert into sqlite3 tables.
    """

    # SQLite3 command templates
    sqlGetMovieData_   = """select * from movies where rowid = {0};"""
    #sqlGetPersonID_    = """select rowid from people where rturl = {0};"""
    #sqlGetActorID_     = """select rowid from actors where peopleid = {0} and movieid = {1};"""
    #sqlGetDirectorID_  = """select rowid from directors where peopleid = {0} and movieid = {1};"""
    #sqlGetCharacterID_ = """select rowid from characters where peopleid = {0} and movieid = {1} and name = {2};"""
    sqlUpdateMoviesKV_ = """update movies set {0} = '{1}' where rowid = {2};"""

    sqlcmd = sqlGetMovieData_.format(movieid)
    results = trySqlcmdFetchall(con,sqlcmd)
    if not results:
        return 'Error'
    row = results[0]
    title,year,releasedate,rtmeterall,rtmetertop,criticconsensus,runtime,rating, \
        ratingnotes,medium,version,genres,studio,synopsis,url = row

    # update meta data
    vars = ["releasedate","rtmeterall","rtmetertop","criticconsensus","runtime","rating", \
                "ratingnotes","genres","studio","synopsis"]
    varlist = [var for var in vars if not eval(var)]
    for var in varlist:
        value = escapeQuotes(str(eval("""metaData['{0}']""".format(var))))
        sqlcmd = sqlUpdateMoviesKV_.format(var,value,movieid)
        trySqlcmdCommit(con,sqlcmd)
    
    return


def updateDirectorsRTDB(movieid,metaData):
    """
    
    """
    sqlSelectRTUrlPeople_ = """select personid, rturl from people where rturl = "{0}";"""  
    sqlUpdateRTUrlPeople_ = """insert into people (credited,rturl) values ("{0}","{1}");"""
    sqlSelectDirectors_ = """select * from directors where personid = {0} and movieid = {1};"""
    sqlInsertDirectors_ = """insert into directors (personid,movieid) values ({0},{1});"""
    
    # update directors
    for person in metaData['directors']:
        credited = person[0]
        rturl = person[1]
        sqlcmd = sqlSelectRTUrlPeople_.format(rturl)
        results = trySqlcmdFetchall(con,sqlcmd)
        if results: personid = results[0][0]
        else:
            sqlcmd = sqlUpdateRTUrlPeople_.format(credited,rturl)
            personid = trySqlcmdCommit(con,sqlcmd)
            
        sqlcmd = sqlSelectDirectors_.format(personid,movieid)
        results = trySqlcmdFetchall(con,sqlcmd)
        if results: print "Director alread exists in DB"
        else:
            sqlcmd = sqlInsertDirectors_.format(personid,movieid)
            rowid = trySqlcmdCommit(con,sqlcmd)



def updateWritersRTDB(movieid,metaData):
    """
    """
    

def populateRTURL(con,movieList,logfname="populateRTURL.log"):
    """                                                             
    Scrape RT URL for a movie and populate tables rows.             
    This is specific to a sqlite3 table which has        
    this schema:                                                    
    
    CREATE TABLE movies (                                           
    title text,                                                     
    year int,                                                       
    releasedate text,                                               
    rtmeterall int,                                                 
    rtmetertop int,                                                 
    criticconsensus text,                                           
    runtime int,                                                    
    rating text,                                                    
    ratingnotes text,                                               
    medium text default "",                                         
    version text default "",                                        
    genres text,                                                    
    studio text,                                                    
    synopsis text,                                                  
    rtmovieurl text,                                                
    unique(title,year,medium,version));                             
    """
    logfile = open(logfname,"a")
    cur = con.cursor()
    sqlGetRowid_ = """select rowid from movies where year = {0} and title = "{1}";"""

    for movie in movieList:
        # Get rowid for movie with given title and year
        sqlGetRowid = sqlGetRowid_.format(movie[0],movie[1])
        rowid_ = cur.execute(sqlGetRowid).fetchall()
        if len(rowid_) != 1:
            msg = "{0}\t{1}\t{2}\t{3}" \
                .format(movie[0],movie[1],'populateRTURL.Error.rowid',datetime.now().isoformat())
            print msg
            logfile.write(msg+"\n")
            return
        try:
            rowid = rowid_[0][0]
        except:
            msg = "{0}\t{1}\t{2}\t{3}" \
                .format(movie[0],movie[1],'populateRTURL.Error.rowid',datetime.now().isoformat())
    
        # Scrape URL from RT website
        result = getMovieURLRT(movie)
        url = result[2]        

        # Add URL data to sqlite3 table
        if "error" not in url.lower():
            try:
                updateTableRowKeyValueString(con,"movies",rowid,"rtmovieurl",url)
            except:
                msg = "{0}\t{1}\t{2}\t{3}".format(result[0],result[1],'Sqlite3.Error',datetime.now().isoformat())
                print msg
                logfile.write(msg+"\n")
                return

        msg = "{0}\t{1}\t{2}\t{3}".format(result[0],result[1],result[2],datetime.now().isoformat())
        print msg
        logfile.write(msg+"\n")

