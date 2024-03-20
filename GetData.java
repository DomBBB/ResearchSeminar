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
public class GetData
{
	// Constant for the Bloomberg API service to be used
	private static final String APIREFDATA_SVC = "//blp/refdata";

	// Main method
	public static void main(String[] args) throws Exception
  	{
    		GetData example = new GetData();
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
           	new String[]{"ADM LN", "EUR", "Equity"},
			new String[]{"AGN NA", "EUR", "Equity"},
			new String[]{"AGS BB", "EUR", "Equity"},
			new String[]{"ALV GR", "EUR", "Equity"},
			new String[]{"ALMB DC", "EUR", "Equity"},
			new String[]{"ASRNL NA", "EUR", "Equity"},
			new String[]{"G IM", "EUR", "Equity"},
			new String[]{"AV/ LN", "EUR", "Equity"},
			new String[]{"CS FP", "EUR", "Equity"},
			new String[]{"BALN SW", "EUR", "Equity"},
			new String[]{"BEZ LN", "EUR", "Equity"},
			new String[]{"CSN LN", "EUR", "Equity"},
			new String[]{"COFA FP", "EUR", "Equity"},
			new String[]{"DFV GR", "EUR", "Equity"},
			new String[]{"DLG LN", "EUR", "Equity"},
			new String[]{"EUC PW", "EUR", "Equity"},
			new String[]{"FBD ID", "EUR", "Equity"},
			new String[]{"GJF NO", "EUR", "Equity"},
			new String[]{"GCO SM", "EUR", "Equity"},
			new String[]{"HNR1 GR", "EUR", "Equity"},
			new String[]{"HUW LN", "EUR", "Equity"},
			new String[]{"HELN SW", "EUR", "Equity"},
			new String[]{"JUST LN", "EUR", "Equity"},
			new String[]{"LRE LN", "EUR", "Equity"},
			new String[]{"LGEN LN", "EUR", "Equity"},
			new String[]{"LDA SM", "EUR", "Equity"},
			new String[]{"MNG LN", "EUR", "Equity"},
			new String[]{"MANTA FH", "EUR", "Equity"},
			new String[]{"MAP SM", "EUR", "Equity"},
			new String[]{"MUV2 GR", "EUR", "Equity"},
			new String[]{"NN NA", "EUR", "Equity"},
			new String[]{"NBG6 GR", "EUR", "Equity"},
			new String[]{"ONDO LN", "EUR", "Equity"},
			new String[]{"PGH LN", "EUR", "Equity"},
			new String[]{"PHNX LN", "EUR", "Equity"},
			new String[]{"PZU PW", "EUR", "Equity"},
			new String[]{"PROT NO", "EUR", "Equity"},
			new String[]{"RGG PZ", "EUR", "Equity"},
			new String[]{"REVO IM", "EUR", "Equity"},
			new String[]{"RLV GR", "EUR", "Equity"},
			new String[]{"SBRE LN", "EUR", "Equity"},
			new String[]{"SAGA LN", "EUR", "Equity"},
			new String[]{"SAMPO FH", "EUR", "Equity"},
			new String[]{"SCR FP", "EUR", "Equity"},
			new String[]{"SFAB SS", "EUR", "Equity"},
			new String[]{"STB NO", "EUR", "Equity"},
			new String[]{"SLHN SW", "EUR", "Equity"},
			new String[]{"SREN SW", "EUR", "Equity"},
			new String[]{"TLX GR", "EUR", "Equity"},
			new String[]{"TOP DC", "EUR", "Equity"},
			new String[]{"TBK RO", "EUR", "Equity"},
			new String[]{"TRYG DC", "EUR", "Equity"},
			new String[]{"UNI IM", "EUR", "Equity"},
			new String[]{"US IM", "EUR", "Equity"},
			new String[]{"UQA AV", "EUR", "Equity"},
			new String[]{"VAHN SW", "EUR", "Equity"},
			new String[]{"VIG AV", "EUR", "Equity"},
			new String[]{"WUW GR", "EUR", "Equity"},
			new String[]{"ZURN SW", "EUR", "Equity"}
        	));

		for (String[] pair : securitiesAndCurrencies) {
            String securityName1 = pair[0];
            String securityCurrency = pair[1];
			String securityName2 = pair[2];

			Request request = refDataService.createRequest("HistoricalDataRequest");
			request.append("securities", securityName1 + " " + securityName2);

			request.append("fields", "PX_LAST");

			request.set("periodicitySelection", "DAILY");
			request.set("startDate", "19000101");
			request.set("endDate", "20231231");
			request.set("currency", securityCurrency);

			System.out.println("Sending Request: " + request);
			session.sendRequest(request, null);
			///////////////////////////////////////////

			// Handle Reply
			try (FileWriter writer = new FileWriter(securityName1.replace("/", "_") + "_" + securityName2 + "_" + securityCurrency + ".csv")) {
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
