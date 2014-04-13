# Some fun made with the bloomberg API
# export DYLD_LIBRARY_PATH=~/Work/bloombergbot/blpapi_cpp_3.7.5.1/Darwin/

import blpapi
import datetime
from optparse import OptionParser

DEFAULT_IP = "10.8.8.1"
DEFAULT_PORT = 8194

dataList = []


def parseCmdLine():
    parser = OptionParser(description="Retrieve reference data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default=DEFAULT_IP)
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=DEFAULT_PORT)

    (options, args) = parser.parse_args()

    return options


def main():
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    print "Connecting to %s:%s" % (options.host, options.port)
    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        print "Failed to start session."
        return

    try:
        # Open service to get historical data from
        if not session.openService("//blp/refdata"):
            print "Failed to open //blp/refdata"
            return

        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")

        # Create and fill the request for the historical data
        request = refDataService.createRequest("HistoricalDataRequest")
        request.getElement("securities").appendValue("SPX Index")
        request.getElement("fields").appendValue("PX_LAST")
        request.getElement("fields").appendValue("OPEN")
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", "DAILY")
        request.set("startDate", "19830101")
        request.set("endDate", "20131231")
        request.set("maxDataPoints", 100)

        print "Sending Request:", request
        # Send the request
        session.sendRequest(request)
        finalmsg = None

        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                print msg
                print (msg.messageType())
                if msg.messageType() == "HistoricalDataResponse":
                	finalmsg = msg


            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                print("Data recieved from server. Begin parsing...")
                fieldData = msg.getElement("securityData").getElement("fieldData")
                fieldIterator = fieldData.values()
                for el in fieldIterator:
                	date = el.getElementAsDatetime("date")
                	priceClose = el.getElementAsFloat("PX_LAST")
                	priceOpen = el.getElementAsFloat("OPEN")

                	store = (date, priceClose, priceOpen)
                	dataList.append(store)

                break
    finally:
        # Stop the session
        session.stop()

    print("Parsing finished! Beginning analysis.")

    i = 0
    hardStopDate = dataList[len(dataList) - 1][0]
    while i < len(dataList):
    	shortDateList = []
    	j = 0 	

    	startDate = dataList[i][0]
    	stopDate = firstDate.replace(year=firstDate.year+1)

    	#Check that we have at least one day's worth of data to examine
    	if stopDate > hardStopDate:
    		break 

    	# Create the price/date list for getBestTime
    	while startDate.date() < stopDate.date():
    		shortDateList.append(dataList[i + j])
    		startDate = dataList[i + j][0]
    		j++

    	# find the best day to buy and sell
    	bestBuy, bestSell = getBestTime(shortDateList)

    	#Convert the dates to (week, day) tuples
    	buyWeek, buyDay = weekDay(bestBuy.year, bestBuy.month, bestBuy.day)
    	sellWeek, sellDay = weekDay(bestSell.year, bestSell.month, bestSell.day)



    	i++
    		
# Given an array of (date, priceClose, priceOpen), return a buydate that aligns with the 
# lowest buy price and a selldate that aligns with the highest sell price
def getBestTime(store):
    min = 0
    maxDiff = 0
    buydate = datetime.date(0, 0, 0)
    selldate = datetime.date(0, 0, 0)
    for i in range (0, len(store)):
        if store[i][2] < store[min][2]:
            min = store[i][2]
        diff = store[i][2] - store[min][2]
        if diff > maxDiff:
            buy = min
            buydate = store[min][0]
            sell = i
            selldate = store[i][0]
            maxDiff = diff
    return (buydate, selldate)

	 


# Given a date, computes week given the month day and year
# https://stackoverflow.com/questions/9847213/which-day-of-week-given-a-date-python/17120430#17120430
def weekDay(year, month, day):
	offset = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]

	afterFeb = 1
	if month > 2: afterFeb = 0
	aux = year - 1700 - afterFeb
	# dayOfWeek for 1700/1/1 = 5, Friday
	dayOfWeek  = 5
	# partial sum of days betweem current date and 1700/1/1
	dayOfWeek += (aux + afterFeb) * 365                  
	# leap year correction    
	dayOfWeek += aux / 4 - aux / 100 + (aux + 100) / 400     
	# sum monthly and day offsets
	dayOfWeek += offset[month - 1] + (day - 1)               
	dayOfWeek %= 7
	return dayOfWeek


if __name__ == "__main__":
    print "SimpleHistoryExample"
    try:
        main()
    except KeyboardInterrupt:
        print "Ctrl+C pressed. Stopping..."

__copyright__ = """
Copyright 2012. Bloomberg Finance L.P.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:  The above
copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""
