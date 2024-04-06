// Bloomberg components globally
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Datetime;
// Java components globally
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// The main class
public class GetCurrency
{
	// Constant for the Bloomberg API service to be used
	private static final String APIREFDATA_SVC = "//blp/refdata";

	// Main method
	public static void main(String[] args) throws Exception
  	{
    		GetCurrency example = new GetCurrency();
    		example.run();
    		System.out.println("Press ENTER to quit");
    		System.in.read();
  	}

	// Run method
	private void run() throws Exception
	{
		// Specify BB connection
		SessionOptions sessionOptions = new SessionOptions();
		sessionOptions.setServerHost("localhost");
		sessionOptions.setServerPort(8194);
		System.out.println("Connecting to " + sessionOptions.getServerHost()
           		 + ":" + sessionOptions.getServerPort());
		// Start BB session
    		Session session = new Session(sessionOptions);
		boolean sessionStarted = session.start();
		if (!sessionStarted)
		{
  			System.err.println("Failed to start session.");
  			return;
		}
		// Open the Service
		if (!session.openService(APIREFDATA_SVC))
		{
  			System.out.println("Failed to open service: " + APIREFDATA_SVC);
  			return;
		}
		Service refDataService = session.getService(APIREFDATA_SVC);

		///////////////////////////////////////////
		/* Construct and Send Request */
		///////////////////////////////////////////
		List<String[]> securitiesAndCurrencies = new ArrayList<>(Arrays.asList(
    		new String[]{"GBPEUR BGN", "EUR", "Curncy"},
    		new String[]{"DKKEUR BGN", "EUR", "Curncy"},
    		new String[]{"ITLEUR BGN", "EUR", "Curncy"},
    		new String[]{"CHFEUR BGN", "EUR", "Curncy"},
    		new String[]{"USDEUR BGN", "EUR", "Curncy"},
    		new String[]{"PLNEUR BGN", "CHF", "Curncy"},
            new String[]{"NOKEUR BGN", "EUR", "Curncy"},
    		new String[]{"DKKEUR BGN", "EUR", "Curncy"},
            new String[]{"NLGEUR BGN", "EUR", "Curncy"},
    		new String[]{"SEKEUR BGN", "EUR", "Curncy"},
    		new String[]{"RONEUR BGN", "EUR", "Curncy"}
        	));

		for (String[] pair : securitiesAndCurrencies) {
            		String securityName1 = pair[0];
            		String securityCurrency = pair[1];
			String securityName2 = pair[2];

			Request request = refDataService.createRequest("HistoricalDataRequest");
			request.append("securities", securityName1 + " " + securityName2);

			request.append("fields", "PX_LAST");

			request.set("periodicitySelection", "DAILY");
			request.set("startDate", "20040101");
			request.set("endDate", "20231231");
			request.set("currency", securityCurrency);

			System.out.println("Sending Request: " + request);
			session.sendRequest(request, null);
			///////////////////////////////////////////

			// Handle Reply
			try (FileWriter writer = new FileWriter(securityName1 + "_" + securityName2 + "_" + securityCurrency + ".csv")) {
				writer.write("name,date,PX_LAST\n"); // Write the header line
				// BASE start
				while (true) {
					Event event = session.nextEvent();
					MessageIterator msgIter = event.messageIterator();
					while (msgIter.hasNext()) {
	      					Message msg = msgIter.next();
						if (msg.messageType().toString().equals("HistoricalDataResponse")) {
                       					parseAndWriteCSV(writer, msg);
                    				}
	      					else {System.out.println(msg);}
					}
					if (event.eventType() == Event.EventType.RESPONSE) {
						break;
					}
				}
				// BASE end
			} catch (IOException e) {
           			e.printStackTrace();
        		}
		}
	}

	private void parseAndWriteCSV(FileWriter writer, Message msg) throws IOException {
        Element securityDataArray = msg.getElement("securityData");
		String name = securityDataArray.getElementAsString("security");
           	Element fieldDataArray = securityDataArray.getElement("fieldData");
           	for (int j = 0; j < fieldDataArray.numValues(); ++j) {
               	Element fieldData = fieldDataArray.getValueAsElement(j);
               	String date = fieldData.getElementAsString("date");
               	double pxLast = fieldData.getElementAsFloat64("PX_LAST");
               	// Write to CSV
               	writer.write(name + "," + date + "," + pxLast + "\n");
        }
		System.out.println("Success");
    }

}
