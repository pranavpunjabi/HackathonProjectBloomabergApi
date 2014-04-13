# Some fun made with the bloomberg API
# export DYLD_LIBRARY_PATH=~/Work/bloombergbot/blpapi_cpp_3.7.5.1/Darwin/

import blpapi
import datetime
from optparse import OptionParser
from dateutil.relativedelta import relativedelta
from collections import Counter

DEFAULT_IP = "10.8.8.1"
DEFAULT_PORT = 8194

dataList = []
bestBuySellArr = []


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
        request.set("startDate", "19200101")
        request.set("endDate", "20131231")
        request.set("maxDataPoints", 100000000)

        print "Sending Request:", request
        # Send the request
        session.sendRequest(request)
        finalmsg = None

        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
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

    analyzeDates()

    print("Analysis complete.")

    buydata = Counter([el[0] for el in bestBuySellArr])
    selldata = Counter([el[1] for el in bestBuySellArr])

    bestBuy = buydata.most_common(1)
    bestSell = selldata.most_common(1)

    bestBuyWeek = bestBuy[0][0][0] 
    bestBuyWeekday = bestBuy[0][0][1] 
    bestSellWeek = bestSell[0][0][0]
    bestSellWeekday = bestSell[0][0][1]

    b = "The best day of the year to purchase stock is on weekday " + str(bestBuyWeekday) + " of week " + str(bestBuyWeek) + "."
    s = "The best day of the year to sell stock is on weekday " + str(bestSellWeekday) + " of week " + str(bestSellWeek) + "."

    print b
    print s

    
# Analyzes the date array
def analyzeDates():
	i = 0
	hardStopDate = dataList[len(dataList) - 1][0]
	while i < len(dataList):
		shortDateList = []
		j = 0 	

		startDate = dataList[i][0]
		stopDate = startDate + relativedelta(year = startDate.year + 1)


		#Check that we have at least one day's worth of data to examine
		if stopDate > hardStopDate:
			break 

		# Create the price/date list for getBestTime
		while startDate< stopDate:
			shortDateList.append(dataList[i + j])
			startDate = dataList[i + j][0]
			j += 1

		# find the best day to buy and sell
		bestBuy, bestSell = getBestTime(shortDateList)

		#Convert the dates to (week, day) tuples
		buyWeek, buyDay = bestBuy.isocalendar()[1], bestBuy.isocalendar()[2]
		sellWeek, sellDay = bestSell.isocalendar()[1], bestSell.isocalendar()[2]

		# Save these in our bestBuyarr/bestSellarr
		buy = (buyWeek, buyDay)
		sell = (sellWeek, sellDay)
		buysell = (buy, sell)

		bestBuySellArr.append(buysell)

		# Bookkeeping
		i += 1
    		
# Given an array of (date, priceClose, priceOpen), return a buydate that aligns with the 
# lowest buy price and a selldate that aligns with the highest sell price
def getBestTime(store):
    mini = 0
    maxDiff = 0
    buydate = store[0][0]
    selldate = store[0][0]
    for i in range (0, len(store)):
        if store[i][2] < store[mini][2]:
            mini = i
        diff = store[i][2] - store[mini][2]
        if diff > maxDiff:
            buy = mini
            selldate = store[i][0]
            buydate = store[mini][0]

    return (buydate, selldate)

	 


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
