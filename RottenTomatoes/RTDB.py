"""
Mostly wrappers for SQL queries on a database to store
scraped Rotten Tomatoes data.
"""

import sys
from datetime import *
from RottenTomatoes import *
import sqlite3 as lite

def escapeQuotes(string):
    string = re.sub(u"\"",u"\"\"",unicode(string))
    string = re.sub(u"\'",u"\'\'",unicode(string))
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


# Update a string column for a given row in a sqlite3 database
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
        statCode = 'Scucces'
        con.commit()
    except:
        if(not quiet): print 'ERROR: ' + sqlcmd
        statCode = 'Error'
    logmsg = u"""[{0},{1},{2}]"""
    msg = logmsg.format(datetime.now().isoformat(),sqlcmd,statCode)
    if(logging): logfile.write(msg+"\n")
    if(not quiet): print msg
    return cur.lastrowid


def updateMovieMetaDataRTDB(con,movieid,metaData,logfile=None,logging=False,quiet=True):
    """
    Scrape RT URL for movie meta data.
    Insert into sqlite3 tables.
    """
    sqlGetMovieData_   = u"""select * from movies where rowid = {0};"""
    sqlUpdateMoviesKV_ = u"""update movies set {0} = '{1}' where rowid = {2};"""

    sqlcmd = sqlGetMovieData_.format(movieid)
    results = trySqlcmdFetchall(con,sqlcmd)
    if not results:
        return 'Error'
    row = results[0]
    title,year,releasedate,rtmeterall,rtmetertop,criticconsensus,runtime,rating, \
        ratingnotes,medium,version,genres,studio,synopsis,url = row

    vars = ["releasedate","rtmeterall","rtmetertop","criticconsensus","runtime","rating", \
                "ratingnotes","genres","studio","synopsis"]
    varlist = [var for var in vars if not eval(var)]
    for var in varlist:
        value = escapeQuotes(unicode(eval(u"""metaData['{0}']""".format(var))))
        
        sqlcmd = sqlUpdateMoviesKV_.format(var,value,movieid)
        trySqlcmdCommit(con,sqlcmd)


    for director in metaData['directors']:
        updateDirectorRTDB(con,movieid,director)
    
    for writer in metaData['writers']:
        updateWritersRTDB(con,movieid,writer)
        
    for actor in metaData['actors']:
        updateActorAndCharacterRTDB(con,movieid,actor)


def updatePersonRTDB(con,credited,rturl):
    """
    """
    sqlSelectRTUrlPeople_ = u"""select personid, rturl from people where rturl = "{0}";"""
    sqlUpdateRTUrlPeople_ = u"""insert into people (credited,rturl) values ("{0}","{1}");"""

    sqlcmd = sqlSelectRTUrlPeople_.format(rturl)
    results = trySqlcmdFetchall(con,sqlcmd)
    if results: personid = results[0][0]
    else:
        sqlcmd = sqlUpdateRTUrlPeople_.format(credited,rturl)
        personid = trySqlcmdCommit(con,sqlcmd)

    return personid

def updateDirectorRTDB(con,movieid,director):
    """
    """
    sqlSelectDirectors_ = u"""select * from directors where personid = {0} and movieid = {1};"""
    sqlInsertDirectors_ = u"""insert into directors (personid,movieid) values ({0},{1});"""

    credited = escapeQuotes(director[0])
    rturl = director[1]
    personid = updatePersonRTDB(con,credited,rturl)

    sqlcmd = sqlSelectDirectors_.format(personid,movieid)
    results = trySqlcmdFetchall(con,sqlcmd)
    if results: print "Director alread exists in DB"
    else:
        sqlcmd = sqlInsertDirectors_.format(personid,movieid)
        rowid = trySqlcmdCommit(con,sqlcmd)


def updateWritersRTDB(con,movieid,writer):
    """
    """
    sqlSelectWriters_ = u"""select * from writers where personid = {0} and movieid = {1};"""
    sqlInsertWriters_ = u"""insert into writers (personid,movieid) values ({0},{1});"""
    
    credited = escapeQuotes(writer[0])
    rturl    = writer[1]
    personid = updatePersonRTDB(con,credited,rturl)
        
    sqlcmd = sqlSelectWriters_.format(personid,movieid)
    results = trySqlcmdFetchall(con,sqlcmd)
    if results: print "Writer alread exists in DB"
    else:
        sqlcmd = sqlInsertWriters_.format(personid,movieid)
        rowid = trySqlcmdCommit(con,sqlcmd)


def updateActorAndCharacterRTDB(con,movieid,actor):
    """
    """
    sqlSelectActors_ = u"""select * from actors where personid = {0} and movieid = {1};"""
    sqlInsertActors_ = u"""insert into actors (personid,movieid) values ({0},{1});"""
    sqlSelectChar_ = u"""select * from characters where actorid = {0} and movieid = {1} and name = "{2}";"""
    sqlInsertChar_ = u"""insert into characters (actorid,movieid,name) values ({0},{1},"{2}");"""

    credited = escapeQuotes(actor[0])
    rturl = actor[1]
    personid = updatePersonRTDB(con,credited,rturl)

    sqlcmd = sqlSelectActors_.format(personid,movieid)
    results = trySqlcmdFetchall(con,sqlcmd)
    if results: print "Actor alread exists in DB"
    else:
        sqlcmd = sqlInsertActors_.format(personid,movieid)
        rowid = trySqlcmdCommit(con,sqlcmd)

    name = escapeQuotes(actor[2])
    if not name:
        print "Character name not available"
        return
    sqlcmd = sqlSelectChar_.format(personid,movieid,name)
    results = trySqlcmdFetchall(con,sqlcmd)
    if results: print "Character already exist in DB"
    else:
        sqlcmd = sqlInsertChar_.format(personid,movieid,name)
        rowid = trySqlcmdCommit(con,sqlcmd)


def getMovieURLAndIdFromDB(con,movie):
    sqlGetMovieInfo_ = u"""select rowid,rtmovieurl from movies where year="{0}" and title ="{1}";"""
    sqlcmd = sqlGetMovieInfo_.format(movie[0],movie[1])
    results = trySqlcmdFetchall(con,sqlcmd,quiet=False)[0]
    if not results:
        print 'Error'
        return None
    movieid,url = results
    return (movieid,url)


def populateMovieMetaData(con,movie):

    result = getMovieURLAndIdFromDB(con,movie)
    if result: 
        movieid,url = result
        if not url:
            print 'Error: getMovieURLAndIdFromDB'
            return
    else:
        print 'Error: getMovieURLAndIdFromDB'
        return

    result = getMovieMetaDataRT(url)
    if result:
        exitCode,metaData = result
    else:
        print 'Error.getMovieMetaDataRT'
        return
    updateMovieMetaDataRTDB(con,movieid,metaData)

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


def populateRTURL(con,movieList,logfname="populateRTURL.log"):
    """
    """
    logfile = open(logfname,"a")
    cur = con.cursor()
    sqlGetRowid_ = u"""select rowid from movies where year = {0} and title = "{1}";"""

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

