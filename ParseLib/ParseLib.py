import urllib2
from bs4 import BeautifulSoup as bs

def stripPunct(to_translate, translate_to=u''):
    """
    """
    not_letters_or_digits = u'!"#%\'()*+,-./:;<=>?@[\]^_`{|}~'
    translate_table = dict((ord(char), translate_to) for char in not_letters_or_digits)
    return to_translate.translate(translate_table)


def get_parent_text(elem):
    """
    """
    parentItems = []
    for item in elem.children:
        if isinstance(item,bs4.element.NavigableString):
            parentItems.append(item)
    justParentText = "".join(parentItems)
    return justParentText.strip()


def getTheSoup(url,bsparser='lxml'):
    """
    """
    try:
        res = urllib2.urlopen(url)
    except (urllib2.URLError, urllib2.HTTPError):
        return None,bs('',bsparser)
    soup = bs(res.read(),bsparser)
    return soup


def resolveURL(url):
    """
    """
    try:
        res = urllib2.urlopen(url)
    except:
        return None
    return res.geturl()

