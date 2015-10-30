#!/opt/local/bin/python
import sys, re, string
import urllib2
from bs4 import BeautifulSoup as bs
import bs4
from nltk import word_tokenize

def stripPunct(to_translate, translate_to=u''):
    not_letters_or_digits = u'!"#%\'()*+,-./:;<=>?@[\]^_`{|}~'
    translate_table = dict((ord(char), translate_to) for char in not_letters_or_digits)
    return to_translate.translate(translate_table)

def get_only_text(elem):
    for item in elem.children:
        if isinstance(item,bs4.element.NavigableString):
            yield item


def getMovieURLRT(movie):
    """ 
    Return RT movie page URL by sending a simple query to the rottentomatoes website.

    Three classes of results from query:
    1. Query lands directly on movie page: identify by 
    Case identified by: The h1 heading in the "main_container" divider has class "title".
    Action in this case: simply return the url of the page.

    2. Query returns a list of relevant matches.
    Case identified by: The h1 heading in the "main_container" divider contains the phrase 
    "Search Results for".
    Action in this case: select the first match in the list of movie results as long as the 
    movie year for that entry agrees with the query, otherwise return an error string
    containing the text "getMovieURLRT.Error".

    3. Query returns no: identify 
    Case identified by: The h1 heading in the "main_container" divider contains the phrase
    "Sorry, no results found for"
    Action in this case: return an error string containing the text "getMovieURLRT.Error".
    """

    base_url = 'http://www.rottentomatoes.com'
    base_search_url = base_url + '/search/?search='

    movie_title = unicode(movie[1])
    movie_title_words = word_tokenize(stripPunct(movie_title))
    movie_year  = movie[0]

    search_url = base_search_url

    for word in movie_title_words:
        search_url = search_url + word.lower() + '+'
    search_url = search_url+str(movie[0])
    try:
        res = urllib2.urlopen(search_url)
    except (urllib2.URLError, urllib2.HTTPError):
        return 'getMovieURLRT.Error.urlopen'
    f = res.read()

    soup = bs(f,'lxml')
    title_words = [word.lower() for word in word_tokenize(soup.find('title').get_text())]
    #print title_words
    if 'search' in title_words:
        ul = soup.find('ul',attrs={"id":"movie_results_ul"})
        

        lis = ul.findAll('li')
        for li in lis:
            li_movie_year = stripPunct(li.find('span',attrs={"class":"movie_year"}).get_text()).strip()
            if(int(li_movie_year) == int(movie_year)):
                li_movie_title = li.find('a',attrs={"class":"articleLink"}).get_text().strip()
                #li_movie_title = word_tokenize(stripPunct(unicode(li.find('a',attrs={"class":"articleLink"}).get_text())))
                #stray = [i for i in set(set(movie_title_words) - set(li_movie_title_words))]
                #stray = stray + [i for i in set(set(li_movie_title_words) - set(movie_title_words))]
                #if(len(stray)==0):
                #print li_movie_title
                url = base_url + li.find('a',attrs={"class":"articleLink"})['href']
                return li_movie_title, li_movie_year, url
            
        return movie_title, movie_year, 'getMovieURLRT.Error.SearchResultError'
    else:
        h1_movie_title = (''.join(get_only_text(soup.find('h1',attrs={"id":"movie-title"})))).strip()
        h1_movie_year = stripPunct(soup.find('h1',attrs={"id":"movie-title"}).find('span',attrs={'class':'h3 year'}).get_text()).strip()
        #for word in movie_title:
        #    if word.lower() not in title_words:
        #        print 'error: inconsistent search result'
        #        return 'queryRTError.SearchResultsError'
        return h1_movie_title, h1_movie_year,res.geturl()

def getMovieMetaDataRT(url):

    keys = ['datePublished',
            'tmeter_all',
            'tmeter_top',
            'criticConsensus',
            'runtime',
            'Rating',
            'RatingNotes']
    metaData = {}
    for key in keys:
        metaData[key] = ''
    


    if('queryRTError' in url):
        return metaData

    try:
        res = urllib2.urlopen(url)
    except (urllib2.URLError, urllib2.HTTPError):
        return 'getMovieMetaDataRT.urlopen'
    soup = bs(res.read(),'lxml')
    
    # Movie info box
    div_movie_info = soup.find("div",attrs={"class":"movie_info"})

    # pull synopsis
    synopsis = div_movie_info.find("p",attrs={"id":"movieSynopsis"}).find(text=True,recursive=False)
    synopsis = synopsis 
    if div_movie_info.find("p",attrs={"id":"movieSynopsis"}).find("span",attrs={"id":"movieSynopsisRemaining"}):
        synopsis = synopsis \
            + div_movie_info.find("p",attrs={"id":"movieSynopsis"}).find("span",attrs={"id":"movieSynopsisRemaining"}).get_text()
        
    # info box table
    table_info = div_movie_info.find("table", attrs={"class":"info"})

    # pull rating
    rating = table_info.find("td",attrs={"itemprop":"contentRating"}).get_text()

    # pull genre(s)
    genre = []
    spans_genre = table_info.findAll("span",attrs={"itemprop":"genre"})
    for span in spans_genre:
        genre.append(span.get_text())

    # pull director(s)
    td_director =  table_info.find("td",attrs={"itemprop":"director"})
    if(td_director):
        director = []
        spans = td_director.findAll("span",attrs={"itemprop":"name"}) 
        for span in spans:  
            director.append(span.get_text())   

    # pull release Date
    td_datePublished = table_info.find("td",attrs={"itemprop":"datePublished"})
    if td_datePublished: datePublished = table_info.find("td",attrs={"itemprop":"datePublished"}).get_text()
    else: datePublished = ''

    # pull production company
    div_left_col = div_movie_info.find("div",attrs={"class":"left_col"})
    div_productionCompany =  div_left_col.find("span",attrs={"itemprop":"productionCompany"})
    if div_productionCompany: productionCompany = div_productionCompany.get_text()
    else: productionCompany = ''
    
    # pull writer(s)
    writer = []
    for row in table_info.findAll("tr"):
        if row.find("td",text=re.compile(".*Written.*")):
            alinks = row.findAll("a")
            for a in alinks:
                writer.append(a.get_text())
            break
    
    time_duration =div_movie_info.find("time",attrs={"itemprop":"duration"})
    if time_duration: runtime = time_duration['datetime']
    else: time_duration = 'P00M'

    print rating
    print genre
    print director
    print writer
    print runtime
    print productionCompany

    #try:
    #    datePublished = soup.find('td', attrs={'itemprop':'datePublished'})['content']
    #except (TypeError,KeyError):
    #    datePublished = '0000-00-00'
    #try:
    #    tmeter_all = re.match(r"width:(.*)%",soup.findAll('div',attrs={'class':'progress-bar'})[0]['style']).group(1)
    #    tmeter_top = re.match(r"width:(.*)%",soup.findAll('div',attrs={'class':'progress-bar'})[1]['style']).group(1)
    #except (TypeError,KeyError):
    #    tmeter='-1'
    #try:
    #    criticConsensus = soup.find('p',attrs={'class':'critic_consensus'}).get_text()
    #except (TypeError,KeyError):
    #    criticConsensus = 'None'
    #try:
    #    runtime = soup.find('time')['datetime']
    #except (TypeError,KeyError):
    #    runtime = 'P000M'

    #metaData['datePublished'] = datePublished
    #metaData['tmeter_all'] = tmeter_all
    #metaData['tmeter_top'] = tmeter_top
    #metaData['criticConsensus'] = criticConsensus
    #metaData['runtime'] = runtime

    return metaData


def getMovieReviewDataRT(url):
    if('queryRTError' in url):
        return
    
    review_link = url+'reviews/'
    print review_link

    try:
        res = urllib2.urlopen(review_link)
    except (urllib2.URLError, urllib2.HTTPError):
        return 'getMovieReviewLinksRT.urlopen'

    soup = bs(res.read(),'lxml')
    pageInfo = soup.find("span",attrs={"class":"pageInfo"}).get_text()
    p = re.compile(".*Page.*(\d).*of.*(\d)")
    m = p.match(pageInfo)
    try:
        onPage   = int(m.group(1))
        numPages = int(m.group(2))
    except IndexError:
        numPages = 1
        onPage   = 1

    for page in range(numPages):
        
        div_review_rows = soup.find('div',attrs={"class":"content"}) \
            .findAll('div',attrs={"class":"review_table_row"})
                
        for div_review in div_review_rows:
            div_top_critic  = div_review.find("div",attrs={"class":"top_critic"})
            top_critic = 'Top Critic' in div_top_critic.get_text().strip()                    

            div_critic_name = div_review.find("div",attrs={"class":"critic_name"})
            if div_critic_name.find("a"): critic_name = div_critic_name.find("a").get_text()
            else: critic_name = ''
            if div_critic_name.find("em"): critic_src  = div_critic_name.find("em").get_text()
            else: critic_src = ''
                    
            div_container = div_review.find("div",attrs={"class":"review_container"})
            div_review_icon = div_container.find("div",attrs={"class":"review_icon"})
            fresh = 'fresh' in div_review_icon.attrs['class']
            review_date = div_container.find("div",attrs={"class":"review_date"}).get_text().strip()
            blurb = div_container.find("div",attrs={"class":"the_review"}).get_text().encode('ascii','ignore')
            if div_container.find("a"): review_url = div_container.find("a")['href']
            else: review_url = ''
            
            print u"{0:<40s} {1:<40s} {2:<7s} {3:<7s} {4:20s}" \
                .format(critic_name,critic_src, str(top_critic), str(fresh), review_date)
            #print "{0:} {1:}".format(review_url,blurb)

        print "Page: {0:}".format(onPage)
        onPage = onPage + 1
        new_url = review_link + '?page=' + str(onPage) + '&sort='

        try:
            res = urllib2.urlopen(new_url)
        except (urllib2.URLError, urllib2.HTTPError):
            return 'getMovieReviewLinksRT.urlopen'
        soup = bs(res.read(),'lxml')
    
    return soup

