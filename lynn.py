import lxml.html as hm
import urllib.request as ur
import urllib.parse as up
import urllib.error as ue
import pyPdf
basic='http://www.alerian.com/'
base_url = 'http://www.alerian.com/news-releases/'
#'http://www.aaii.com/stock-investor-pro/SIProUpdates?filename=/archive/fullupdate20030703.exe'
#'http://www.renderx.com/demos/examples.html'
# fetch the page
res = ur.urlopen(base_url)
#print(res.read())
# parse the response into an xml tree
page = str(res.read())#.decode("utf-8"))
res.close()
tree = hm.fromstring(page)
# construct a namespace dictionary to pass to the xpath() call
# this lets us use regular expressions in the xpath
ns = {'re': 'http://exslt.org/regular-expressions'}
# iterate over all <a> tags whose href ends in ".pdf" (case-insensitive)
for node in tree.xpath('//a[re:test(@href, "\.pdf$", "i")]', namespaces=ns):
	# print the href, joining it to the base_url
#	print(up.urljoin(base_url, node.attrib['href']))
	alurl=up.urljoin(base_url, node.attrib['href'])
	path=up.urlsplit(node.attrib['href'])[2]
#	print(path)
	filename=path.split('/')
	print(filename[3])
	input1 = PdfFileReader(file(filename[3], "rb"))
	print(input1.getNumPages())
	alres.close()
#page.split("\n")
#print(tree)