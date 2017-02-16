import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
#from lxml import html,etree
import re, os
from time import time
from bs4 import BeautifulSoup
from urlparse import urljoin
import Levenshtein

validCount = 0
inValidCount = 0

try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set() 
    if not os.path.exists("successful_urls.txt") else 
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000

@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "48123229_71169660"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR W17 Grad 48123229, 71169660"
		
        self.frame = frame
        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        print "downloaded ", len(url_count), " in ", time() - self.starttime, " seconds."
        pass

def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))

def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas
    
#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''
def extract_next_links(rawDatas):
    global validCount
    global inValidCount
    writeThrehold = 5
    outputLinks = list()
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.

    Suggested library: lxml
    '''
    for entryInRaw in rawDatas:
        tokens = re.compile("(//|.ics.uci.edu)").split(entryInRaw.url)
        if  re.match(".*\.(txt|css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                 + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|pps|ppt" \
                 + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                 + "|thmx|mso|arff|rtf|jar|csv" \
                 + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", entryInRaw.url.lower()):
            inValidCount += 1
            continue

        # double check if the url is under ics.uci.edu domain
        if tokens [3] == ".ics.uci.edu":
            subdomain = tokens[2]
            if subdomain != "www":
                updateSubdomainLog(subdomain.lower())
            # log invalid/valid links fetched from frontiers
            if is_valid(entryInRaw.url):
                # logValidLinks(entryInRaw.url);
                validCount += 1
                if validCount >= writeThrehold:
                    updateStatics(validCount, True)
                    validCount = 0
            else:
                print "still invalid>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
                inValidCount += 1

            if inValidCount >= writeThrehold:
                updateStatics(inValidCount, False)
                inValidCount = 0

            # that is an error
            if entryInRaw.http_code >= 400:
                print ("Error: Code {0}, MSG = {1}").format(entryInRaw.http_code, entryInRaw.error_message)
                inValidCount += 1
                continue
            # some of the pages have sort features that implemented by redirection and query string
            # e.g.: http://www.ics.uci.edu/~minhaenl?C=N;O=D
            if entryInRaw.is_redirected:
                if entryInRaw.final_url is entryInRaw.url:
                    inValidCount += 1
                    continue

            bsObj = BeautifulSoup(entryInRaw.content, "lxml")
            bsObjStr = str(bsObj).lower()
            duplicate = False
            try:
                # if bsObjStr != "":
                start = bsObjStr.index("<html")
                end = bsObjStr.index("</html>", start) + len("</html>")
                newContent = bsObjStr[start:end]
                duplicate = compareContent(newContent)
            except ValueError:
                print "* Error String: " + bsObjStr
                inValidCount += 1
                continue

            # skip this url if it is considered duplicate as what we processed before
            if duplicate:
                inValidCount += 1
                continue

            links = bsObj.findAll('a', href=re.compile("^[^#]+$"))
            # print ("I have {0} out links").format(len(links))
            updateNumOfOutlink(entryInRaw.url, len(links))
            for link in links:
                absoluteURL = urljoin(entryInRaw.url, link['href'])
                outputLinks.append(absoluteURL)

    return outputLinks

# TRAP_DOMAIN = {"archive.ics.uci.edu/ml",
#                  "calendar.ics.uci.edu",
#                  "fano.ics.uci.edu",
#                  "ganglia.ics.uci.edu",
#                  "arcus-3.ics.uci.edu",
#                  "cbcl.ics.uci.edu"
#                  }

def is_valid(url):
    global validCount
    global inValidCount
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False

    # for trap in TRAP_DOMAIN:
    #     if trap in url:
    #         if trap != "":
    #             print "* Filter1 failed"
    #             return False

    # ORDER is IMPORTANT
    regStrLists = list()
    # ignore some substring
    regStrLists.append(r"^.*(=login|php\?|mailto|\/\.)+.*$")
    # solve continuously ".."
    regStrLists.append(r"^.*\.{2,}.*$")
    # # solve "///" 3+
    regStrLists.append(r"^.*\/{3,}.*$")

    # deal with "/community/news/press/view_press?id=222/blahblah" infinity loop
    # query string should not followed by other path
    regStrLists.append(r"^.*(\?id=)\d+\/+.*$")
    # deal with git repository like query
    regStrLists.append(r"^.*\?.*(\/repository\/|diff|version|revision)+.*$")
    # ignore query string over 80 characters
    # regStrLists.append(r"^.*\?.{80,}$")

    # detect if a group of (at least)10 alphanumeric characters(and underscore, slash) repeat in url,
    # deal with infinity url loop
    regStrLists.append(r"^.*([\w/]{10,}).*\1.*$")

    for index in range(len(regStrLists)):
        if re.compile(regStrLists[index]).search(url):
            print ("* Filter2 failed @regex#{0}").format(index)
            return False

    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)


def compareContent(bsContent):
    startTime = time()
    print "* Comparing Contents By Levenshtein Distance..."
    try:
        contentFile = open("visited_contents.txt", "r")
    except:
        contentFile = open("visited_contents.txt", "w")
        # contentFile.write("0")
        contentFile.close()
        contentFile = open("visited_contents.txt", "r")
    # numOfContent = int(contentFile.readline().strip())


    allOldContent = findContent(contentFile)
    contentFile.close()
    needUpdate = False
    ret = False
    for content in allOldContent:
        ratio = Levenshtein.ratio(bsContent, content)
        # matching threshold is set to 85%, over 90% means "equal"
        if ratio > 0.85:
            print "* Similarity Ratio is {0}".format(ratio)
            # print ("content in File: {0}, bsContent: {1}").format(content, bsContent)
            ret = True
            # if matching, but the similarity is less than 95%, should add new content in the history
            if ratio < 0.95:
                needUpdate = True
            break
        else:
            print "* No Duplicate Found"
            ret = False
    # finnish loop, check if we need update content history or not
    if needUpdate or (ret == False):
        print "* Updating Content History Needed"
        contentFile = open("visited_contents.txt", "a")
        contentFile.write("\n\nFor Readability\n\n")
        contentFile.write(bsContent + "\n")
        contentFile.close()
    endTime = time()
    # print ("* allOldContent has {0} contents in cache").format(len(allOldContent))
    # if contents cache exceed 100 items, clear the file, ignore the one we just added
    if len(allOldContent) > 20:
        contentFile.close()
        contentFile = open("visited_contents.txt", "w")
        contentFile.write("0")
        contentFile.close()
        print "* Contents Cache Reset."

    print("* Comparing Done! Time Elapsed: {0}s").format(endTime - startTime)
    return ret


def findContent(file):
    parsing = False
    contentCount = -1
    contentList = list()
    for line in file:
        if not parsing:
            if line.startswith("<html"):
                contentCount += 1
                contentList.append(line)
                parsing = True
        elif line.startswith("</html>"):
            contentList[contentCount] = contentList[contentCount] + line
            parsing = False
        else:
            contentList[contentCount] = contentList[contentCount] + line
    return contentList


def eHandlerForStatics():
    newLines = "\*\n* Statics for WebCrawler\n*/\n\nNumber of Valid links: 0\nNumber of Invalid links: 0\nPage with the most out links: \nMaximum number of out links: 0\n"
    staticsFile = open("statics.txt", "w")
    staticsFile.write(newLines)
    staticsFile.close()

def updateStatics(increaseNum, valid):
    print "* Updating Statics"
    try:
        staticsFile = open("statics.txt", "r")
    except:
        eHandlerForStatics()
        staticsFile = open("statics.txt", "r")
    lines = staticsFile.readlines()
    if not valid:
        numOfInvalidLinks = re.sub('[^\d]+', '', lines[5])
        lines[5] = "Number of Invalid links: " + str(int(numOfInvalidLinks) + increaseNum) + "\n"
        # print lines[5].rstrip()
    else:
        numOfValidLinks = re.sub('[^\d]+', '', lines[4])
        lines[4] = "Number of Valid links: " + str(int(numOfValidLinks) + increaseNum) + "\n"
        # print "test:" + lines[4].rstrip()
    staticsFile.close()

    # why the "r+" mode append?????????????? I don't want!
    staticsFile = open("statics.txt", "w")
    staticsFile.writelines(lines)
    staticsFile.close()


def updateNumOfOutlink(url, newNum):
    print "* Updating Outlinks"
    try:
        staticsFile = open("statics.txt", "r")
    except:
        eHandlerForStatics()
        staticsFile = open("statics.txt", "r")
    lines = staticsFile.readlines()
    maxNum = re.sub('[^\d]+', '', lines[7])
    if newNum > int(maxNum):
        lines[6] = "Page with the most out links: " + url + "\n"
        lines[7] = "Maximum number of out links: " + str(newNum) + "\n"
        print lines[7].rstrip()
    staticsFile.close()

    # why the "r+" mode append?????????????? I don't want!
    staticsFile = open("statics.txt", "w")
    staticsFile.writelines(lines)
    staticsFile.close()


def updateSubdomainLog(subdomain):
    print "* Updating Subdomain"
    try:
        subdomainFile = open("log_visited_subdomain.txt", "r")
    except:
        subdomainFile = open("log_visited_subdomain.txt", "w")
        subdomainFile.close()
        subdomainFile = open("log_visited_subdomain.txt", "r")
    lines = subdomainFile.readlines()
    notFound = True
    subdomainDict = dict()
    for line in lines:
        item = re.sub('[^a-zA-Z]+', '', line)
        freq = re.sub('[^\d]+', '', line)
        # print "item:{0}".format(item)
        # print "freq:{0}".format(freq)
        if subdomain == item:
            notFound = False
            updatedFreq = int(freq) + 1
        else:
            updatedFreq = int(freq)
        subdomainDict[item] = updatedFreq

    if notFound:
        subdomainDict[subdomain] = int(1)
    subdomainFile.close()

    subdomainFile = open("log_visited_subdomain.txt", "w")
    for key in sorted(subdomainDict.iterkeys()):
        writeline = key.ljust(30) + str(subdomainDict[key]) +"\n"
        subdomainFile.write(writeline)


def logValidLinks(url):
        validLinkFile = open("log_valid_links.txt", "a+")
        validLinkFile.write(url + "\n")

