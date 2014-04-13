# Some fun made with the bloomberg API
# export DYLD_LIBRARY_PATH=~/Work/bloombergbot/blpapi_cpp_3.7.5.1/Darwin/

import blpapi
from optparse import OptionParser

DEFAULT_IP = "10.8.8.1"
DEFAULT_PORT = 8194


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
        request.set("startDate", "19500101")
        request.set("endDate", "19701231")
        request.set("maxDataPoints", 100)

        print "Sending Request:", request
        # Send the request
        session.sendRequest(request)

        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:


                print msg

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                print("Data recieved from server.")
                break
    finally:
        # Stop the session
        session.stop()

    print("Begin ananlys section")
    



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
