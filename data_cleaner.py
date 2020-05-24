# Importazione delle librerie
import logging
import csv
import os

# Configurazione dell'applicativo
root_path = os.getcwd()
#log_file = os.path.join(root_path, 'application.log')
#logging.basicConfig(filename=log_file, filemode='a',
#                    format='%(asctime)s - %(message)s', level=logging.INFO)

dataset_dir = os.path.join(root_path, 'historical_stocks')
hsp_file = os.path.join(dataset_dir, 'historical_stock_prices.csv')
hsp_cleaned = os.path.join(dataset_dir, 'hsp_cleaned.tsv')
hss_file = os.path.join(dataset_dir, 'historical_stocks.csv')
hss_cleaned = os.path.join(dataset_dir, 'hss_cleaned.tsv')


with open(hsp_file) as hsp:
    csv_reader_hsp = csv.reader(hsp)
    next(csv_reader_hsp)
    with open(hsp_cleaned, 'w', encoding='utf-8') as new_hsp:
        csv_writer_hsp = csv.writer(new_hsp, delimiter='\t')
        for row in csv_reader_hsp:
            date = int(row[7].split('-')[0])
            if(2007 < date < 2019):
                line = (row[0], row[2], row[4], row[5], row[6], row[7])
                csv_writer_hsp.writerow(line)
                print(line)


with open(hss_file) as hss:
    csv_reader_hss = csv.reader(hss)
    next(csv_reader_hss)
    with open(hss_cleaned, 'w', encoding='utf-8') as new_hss:
        csv_writer_hss = csv.writer(new_hss, delimiter='\t')
        for row in csv_reader_hss:
            line = (row[0], row[1], row[2], row[3])
            csv_writer_hss.writerow(line)
            print(line)
