# WebScraping

Webscraping routines for personal projects. These python routines depend on Beautiful Soup (bs4) and natural langu\
age toolkit (nltk).

Some wrapper code has been written to do SQL queries on a database to facilitate storing the scraped data. Currently wrapped around SQLite3 for simplicity (Note: update this to use MySql instead).

#### RottenTomatoes (RT)

The following functions are defined:

**getMovieURLRT**([array]movie) - Performs a query on a movie title and movie year returning URL the best match RT movie web page. Failing that it returns an error code.  Input is an [array]movie = ([string]year,[string]title).
 
**getMovieMetaDataRT**([string]url) - Given a RT movie page URL, return a dictionary of movie meta data including:
 - date movie was published
 - movie runtime
 - tomato meter for all critics
 - tomato meter for top critics
 - text blurb for critic consensus
 - list of writer(s)
 - list of director(s)
 - list of actors and characters
 - name of production company
 - list of movie genre(s)
 - movie rating
 - movie rating description text

**getMovieReviewLinksRT**([string]url) - returns critic review information including:
 - critic name
 - publication source
 - review text blurb
 - fresh or rotten [boolean]
 - top cricit flag [boolean]
 - URL to full review


#### NFL_News

