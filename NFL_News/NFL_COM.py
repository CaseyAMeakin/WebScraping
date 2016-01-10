"""
Functions and data for parsing news articles on the nfl.com website.
"""

from ParseLib import *

writers = [ ('0ap3000000607517','LaVar Arrington'),\
                ('0ap3000000566507','Brian Baldinger'),\
                ('0ap1000000233164','Judy Battista'),\
                ('09000d5d8220cff3','Brian Billick'), \
                ('09000d5d80026e07','Gil Brandt'), \
                ('09000d5d81b52bfe','Albert Breer'), \
                ('09000d5d80f97bfd','Bucky Brooks'), \
                ('09000d5d826ca305','Charley Casserly'), \
                ('0ap3000000508550','Jeffri Chadiha'), \
                ('0ap1000000069288','Stacey Dales'), \
                ('09000d5d81a94f23','Dave Dameshek'), \
                ('09000d5d82295a9d','Jeff Darlington'), \
                ('0ap1000000126479','Charles Davis'), \
                ('09000d5d800219d7','Michael Fabiano'), \
                ('0ap2000000364344','Matt Franciscovich'), \
                ('0ap3000000607520','Akbar Gbaja-Biamila'), \
                ('0ap1000000050603','Akbar Gbajabiamila'), \
                ('0ap2000000363405','Alex Gelhar'), \
                ('0ap1000000217475','Chase Goodbread'), \
                ('0ap2000000347358','Marcas Grant'), \
                ('09000d5d823c1aa0','Dan Hanzus'), \
                ('0ap3000000503108','Matt Harmon'), \
                ('09000d5d82036444','Elliot Harrison'), \
                ('09000d5d82906849','Daniel Jeremiah'), \
                ('09000d5d8280fab0','Kimberly Jones'), \
                ('09000d5d828f2127','Aditi Kinkhabwala'), \
                ('0ap1000000371814','Andrea Kremer'), \
                ('09000d5d82aec709','Mark Kriegel'), \
                ('0ap3000000531998','Colin J. Liotta'), \
                ('09000d5d804a11cd','Mike Mayock'), \
                ('0ap3000000531355','Willie McGinest'), \
                ('0ap3000000407741','Conor Orr'), \
                ('0ap3000000512839','Dan Parr'), \
                ('0ap1000000236291','Kevin Patra'), \
                ('0ap2000000327459','Desmond Purnell'), \
                ('09000d5d82035eb2','Adam Rank'), \
                ('09000d5d8280f9d4','Ian Rapoport'), \
                ('0ap2000000345165','Jim Reineking'), \
                ('09000d5d82610475','Chad Reuter'), \
                ('09000d5d827ba779','Gregg Rosenthal'), \
                ('09000d5d82af3236','Adam Schein'), \
                ('09000d5d823c1a70','Marc Sessler'), \
                ('0ap1000000232099','Michael Silver'), \
                ('0ap1000000237971','NFL UP! Ambassador'), \
                ('0ap1000000132626','Chris Wesseling'), \
                ('0ap3000000500464','Colleen Wolfe'), \
                ('09000d5d80a8a933','Steve Wyche'),\
                ('0ap3000000458453','Lance Zierlein')]          

writers_dict = dict((y,x) for x,y in writers) 

"""
"""


def getAuthorID(authorName):
    """
    """
    authorId = writers_dict[authorName]
    return authorId


def getAuthorURL(authorName):
    """
    """
    authorId = writers_dict[authorName]
    return u"""http://www.nfl.com/news/author?id={0}&fullName={1}""".format(authorId,authorName)


def getAuthorNameBlurb(authorUrl):
    """
    """
    soup = getTheSoup(authorUrl)
    print 'Got the soup'

    col1 = None
    name = None
    blurb = None
    try:
        col1 = soup.find('div',attrs={'class':'span-11'})
        h2  = col1.find('h2')
        name = h2.get_text()

        pss = col1.find_all('p')
        ps = []
        blurb = u""        
        for p in pss: 
            ppar = p.parent
            if 'span-11' in p.parent.get('class') and u"\xa0" not in p: 
                blurb += p.get_text()
        
    except:
        pass
    
    return name,blurb
