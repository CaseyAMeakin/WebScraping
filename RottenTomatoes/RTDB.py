"""
Mostly wrappers for SQL queries on a database to store
scraped Rotten Tomatoes data.
"""

import sys
from datetime import *
from RottenTomatoes import *
import sqlite3 as lite

# Populate list of movie data: [Array](year, title) 
def readMovieList(movie_list_file):
    movie_list = open(movie_list_file)
    movies = []
    for line in movie_list:
        year = line.split()[0]
        title = line.split(year + '\t')[1].strip()
        #print year, title
        movies.append((year,title))
    return movies

# Connect to DB return cursor
def connectDB(sqlite3_db_file_name):
    con = lite.connect(sqlite3_db_file_name)
    return con


# Insert Title and Year into table "movie"
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

# Update a string column for a given row
def updateTableRowKeyValueString(con,table,rowid,key,stringValue):
    cur = con.cursor()
    sql_ = """update {0} set {1} =  "{2}" where rowid = {3};"""
    sqlcmd = sql_.format(table,key,stringValue,rowid)
    
    result = con.execute(sqlcmd)
    con.commit()


def insertRTMetaData(con,metaData):
    """
    Insert metaData into sqlite3 tables
    """
    
    pass


def populateRTMetaData(con,movieList,logfname="populateRTMetaData.log"):
    """
    Scrape RT URL for movie meta data.
    Insert into sqlite3 tables.
    """
    logfile=open(logfname,"write")
    logmsg = """[{0},{1},{2}]"""
    
    cur = con.cursor()
    
    # SQLite3 command templates
    sqlGetMovieData_   = """select rowid,* from movies where year = {0} and title = "{1}";"""
    sqlGetPersonID_    = """select rowid from people where rturl = {0};"""
    sqlGetActorID_     = """select rowid from actors where peopleid = {0} and movieid = {1};"""
    sqlGetDirectorID_  = """select rowid from directors where peopleid = {0} and movieid = {1};"""
    sqlGetCharacterID_ = """select rowid from characters where peopleid = {0} and movieid = {1} and name = {2};"""
    

    for movie in movieList:

        sqlGetMovieData = sqlGetMovieData_.format(movie[0],movie[1])
        results = cur.execute(sqlGetMovieData).fetchall()

        if results:
            msg = logmsg.format(datetime.now().isoformat(),sqlGetMovieData,"Success")
            logfile.write(msg+"\n")
            row = results[0]
            movied,title,year,releasedate,rmeterall,rmeterrop,criticconcensus,runtime,rating, \
                ratingnotes,medium,version,genres,studio,synopsis,url = row
            # Scrape Metadata from RT website
            (exitCode,metaData) = getMovieMetaDataRT(url)
            msg = logmsg.format(datetime.now().isoformat(),"getMovieMetaDataRT("+url+")",exitCode)
            logfile.write(msg+"\n")
            print exitCode.lower()
            if re.search(".*success.*",exitCode.lower()):
                
                #exitCode = insertRTMetaData(con,movieid,metaData)
                #msg = logmsg.format(datetime.now().isoformat(),"insertRTMetaData(con,metaData)",exitCode)
                #logfile.write(msg+"\n")
                #print msg

                # First, populate meta data in "movies" table
                print releasedate
                
                
                
        else:
            msg= logmsg.format(datetime.now().isoformat(),sqlGetMovieData,"Error.noSqlResult")
            logfile.write(msg+"\n")

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

