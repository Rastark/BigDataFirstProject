package mapreduce.reducer;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.finput.HsHspJoinFields;
import mapreduce.objects.CompanyData;
import mapreduce.objects.HsHspJoinWritable;
import mapreduce.objects.StringBigram;

public class SectorYearReducer extends Reducer<StringBigram, HsHspJoinFields, Text, Text> {

    public void reduce(Text key, Iterable<HsHspJoinWritable> values, Context context)
            throws IOException, InterruptedException {

        // Mean volume variables
        double sumVolume = 0;
        int countVolume = 0;
        double meanVolume = 0;

        // Quotation change variables
        double quotationChange = 0;
        HashMap<String, CompanyData> companyDataMap =  new HashMap<>();
        double sumOfChanges = 0;

        // Mean daily quotation variable
        double dailyQuotationSum = 0;
        double meanDailyQuotation = 0;
        int count = 0;

        for (HsHspJoinWritable val : values) {

            // Mean volume parameters
            sumVolume += val.getVolume().get();
            countVolume++;

            // Quotation change parameters
            int currentDay = val.getDayInt().get();
            int currentClose = val.getClose().get();
            String ticker = val.getTicker().toString();
            
            if(!companyDataMap.containsKey(ticker)) {
                companyDataMap.put(ticker, new CompanyData());
            }
            CompanyData currentCompanyData = companyDataMap.get(ticker);

            if(currentCompanyData.getFirstDay() > currentDay) {
                currentCompanyData.setFirstDay(currentDay);
                currentCompanyData.setFirstClose(currentClose);
            } 
            if(currentCompanyData.getLastDay() < currentDay) {
                currentCompanyData.setLastDay(currentDay);
                currentCompanyData.setLastClose(currentClose);
            }

            //Mean daily quotation parameters
            dailyQuotationSum += currentClose;
            count++;
        }

        for (String t : companyDataMap.keySet()) {
            CompanyData cd = companyDataMap.get(t); 
            double currentChange = (cd.getLastClose() - cd.getFirstClose()) / cd.getFirstClose() * 100;
            sumOfChanges += currentChange;
        }
        
        meanVolume = sumVolume / countVolume;

        quotationChange = sumOfChanges / companyDataMap.size();

        meanDailyQuotation = dailyQuotationSum / count;

        context.write(new Text(key), new Text(meanVolume + ", " + quotationChange + ", " + meanDailyQuotation));
    }
}