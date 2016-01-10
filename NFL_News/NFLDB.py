"""
Wrappers for SQL queries on a database to store 
NFL news article data.
"""
from WebsScraping.db.mysql import *
from NFL_News import *
from ParseLib import *


def populateAuthorsNFLCOM(con,authorNameList):
    """
    """
    cur = con.cursor()
    sql_ = u"""insert into authors (credited,blurb,url) values ({0},{1},{2})"""
    for authorName in authorNameList:
        try:
            url = getAuthorURL(authorName)
            name,blurb = getAuthorNameBlurb(url)
            if name and blurb and url:
                sqlcmd = sql_.format(name,blurb,url)
        except:
            pass
        
