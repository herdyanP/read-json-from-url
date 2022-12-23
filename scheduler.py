import datetime
import json
import time

import requests
import schedule
import yaml

import mysql.connector
import logging

logdate = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
logging.basicConfig(filename=f'{logdate}.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

statSuccess = True
data = None

def init():
    global statSuccess
    statSuccess = False
    start()

def start():
    global statSuccess
    now = datetime.datetime.now()
    wk = now.isoweekday()

    if(wk <= 5): # biar cuma jalan di weekday
        while(statSuccess == False):
            # Connect ke database
            printWithStamp('Connecting to database', end = '...', flush=True)
            try:
                mydb = mysql.connector.connect(
                    host=config['dbHost'],
                    user=config['dbUser'],
                    password=config['dbPass'],
                    database=config['dbName']
                )
                print('Done')

                # Fetch data dari API
                dtformat = now.strftime('%Y%m%d')
                printWithStamp(f'Fetching JSON {dtformat}', end='...', flush=True)
                try:
                    response = json.loads(requests.get(f"http://{config['apiUrl']}:{config['apiPort']}/webservice/mcollection/InqData?token={config['apiToken']}&x=1&y=90000").text)
                    print(f'{response["msg"]}')

                    # Insert ke database
                    printWithStamp(f'Processing data', end = '...', flush=True)
                    
                    try:
                        mycur = mydb.cursor()

                        line_count = 0
                        dtnow = now.strftime('%Y-%m-%d')

                        sql = f'TRUNCATE TABLE saldotabungan_tmp'
                        mycur.execute(sql)

                        for data in response['data']:
                            namaclear = data['NAMA'].replace("'", "")
                            if data['ALAMAT'] is not None:
                                alamclear = data['ALAMAT'].replace("'", "")

                            sql = f'INSERT INTO saldotabungan_tmp (CIF, SSREK, SSNAMA, SSALAMAT, SSSALDO, SSTGL, JPINJAMAN, TGLDATA) VALUES ("{data["CIF"]}", "{data["NO_REKENING"]}", "{namaclear}", "{alamclear}", "{data["SALDO"]}", "{dtnow}", "{data["JENIS_REKENING"]}", "{data["TGL_DATA"]}");'
                            mycur.execute(sql)

                            line_count += 1

                        # Pindah data non-duplikat dari saldotabungan_tmp ke saldotabungan
                        sql = f'INSERT INTO saldotabungan (CIF, SSREK, SSNAMA, SSALAMAT, SSSALDO, SSTGL, SSUSERID, SSTGLSTAMP, JPINJAMAN, `STATUS`, TGLDATA) SELECT CIF, SSREK, SSNAMA, SSALAMAT, SSSALDO, SSTGL, SSUSERID, SSTGLSTAMP, JPINJAMAN, `STATUS`, TGLDATA FROM saldotabungan_tmp WHERE SSREK NOT IN (SELECT SSREK FROM saldotabungan WHERE SSTGL = "{dtnow}");'
                        mycur.execute(sql)
                        
                        print('Done')
                        printWithStamp(f'Processed {line_count} lines')

                        mydb.commit()
                        mycur.close()

                        if(line_count > 0):
                            statSuccess = True

                    except Exception as e:
                        print('Fail')
                        logging.exception('Exception occured when inserting to database!')
                        quit()
                except:
                    print('Fail')
                    logging.exception('Exception occured when fetching JSON!')
            except Exception as e:
                print('Fail')
                logging.exception('Exception occured when connecting to database!')

            time.sleep(600)

def printWithStamp(*args, **kwargs):
    dt = datetime.datetime.now()
    ts = dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f'({ts}) ' + ' '.join(map(str,args)), **kwargs)


try:
    config = yaml.safe_load(open('config.yml'))
except Exception as e:
    logging.exception('Exception occured')

    time.sleep(10)
    quit()

# ========== MAIN CODE HERE ===========
print("====== AUTO-UPLOADER MCOLLECTION JOGJA ======")
schedule.every().day.at("06:00").do(init)
while True:
    schedule.run_pending()
    time.sleep(1)

# buat compile:
# pyinstaller --onefile --clean [nama_file]