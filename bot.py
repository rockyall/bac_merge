import shutil
import sys
import os
import csv
import urllib
import urllib3
import time
import pyodbc
from datetime import date, datetime
from dbServer import db_server
from bac import bac_credomatic

from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

download_path = os.path.dirname(os.path.abspath(__file__))

download_config = {
    'download.default_directory': download_path
}


_options = Options()
_options.page_load_strategy = 'eager'
_options.add_experimental_option('prefs', download_config)

pathWin = r"c:\WebDriver\bin\chromedriver"
pathLin = r"/opt/WebDriver/bin/chromedriver"
driver = Chrome(executable_path=pathWin, options=_options)


def save_data_sqlserver(server_name, db, csvpath, username, password):
    try:
        SqlServer = db_server()
        SqlServer.get_mysql_connection(server_name, db, username, password)
        Bac = bac_credomatic(SqlServer, csvpath)

        profile_id = Bac.merge_transaction_profile()
        Bac.merge_transaction(profile_id)

        # Finnally we copy the file to our assets, for backups propuses
        location_csvPath2 = f"/assets/{csvpath}"
        if(os.path.exists(csvpath)):
            if(os.path.exists(location_csvPath2)):
                csvFile = os.stat(csvpath)
                date1 = datetime.utcfromtimestamp(csvFile.st_mtime)

                csvFileAssets = os.stat(location_csvPath2)
                date2 = datetime.utcfromtimestamp(csvFileAssets.st_mtime)

                if(date1 > date2):
                    shutil.copy(src=csvpath, dst=f"./assets")
            else:
                shutil.copy(src=csvpath, dst=f"./assets")
        SqlServer.close_connection()
    except Exception as ex:
        print(f"An exeption occure in line 61 in bot.py {ex}")


def find_and_download_transactions():
    try:
        # Open new windows to search the account balance
        url_link = "https://www1.sucursalelectronica.com/ebac/module/accountbalance/accountBalance.go"
        # url_link = "https://www6.sucursalelectronica.com/ebac/module/accountbalance/accountBalance.go"
        driver.execute_script("window.open();")

        windows_list = driver.window_handles
        driver.switch_to.window(windows_list[1])
        driver.get(url_link)

        # This function will detect and set the date from we want to download the data.
        fromDate = get_last_transaction_date()
        # fromDate = datetime(fromDate.year, fromDate.month, fromDate.day + 1)
        select_calendar_date(fromDate, datetime.now())

        down_name = "download"
        bt_queryID = "normalQueryButton"

        el_query_li = driver.find_element_by_id(bt_queryID)
        el_query_li.find_element_by_class_name("defaultBtn").click()
        time.sleep(2)
        driver.find_element_by_name(down_name).click()
        driver.find_element_by_xpath(
            "/html/body/div[1]/div[1]/div[2]/ul/li[3]/a").click()
        time.sleep(1)
        driver.close()
        driver.switch_to.window(windows_list[0])

    except Exception as ex:
        print("\n\n" + ex)


def get_last_transaction_date():
    SqlServer = db_server()
    SqlServer.get_mysql_connection("localhost", "db_finance", "root", "root")
    sqlString = "select * from transactionsdetails order by Date desc Limit 1"
    list = SqlServer.get_data_query_list(sqlString)
    if(list == 0):
        return datetime.now()
    data = list[0]
    return data[1]  # the date time index to get the date object


def select_calendar_date():
    try:
        calendar_id = "initDate"
        el_calendar = driver.find_element_by_id(calendar_id)
        el_calendar.click()

        # Detect the elements values
        css_calendar = "ui-datepicker-calendar"
        css_calendar_back = "ui-datepicker-prev"
        css_calendar_forward = "ui-datepicker-next"

        # loop to find the the oldest date posible
        while True:
            el_calendar_back = driver.find_element_by_class_name(
                css_calendar_back)
            el_calendar_back.click()
            isDisable_back = driver.find_elements_by_css_selector(
                f"a.{css_calendar_back}.ui-state-disabled")

            if(len(isDisable_back) <= 0):
                continue

            el_calendar = driver.find_element_by_class_name(css_calendar)
            rows = el_calendar.find_elements_by_tag_name('tr')

            try:
                for x in rows:
                    columns = x.find_elements_by_tag_name('td')
                    for y in columns:
                        if y.text == '1':
                            el_a = y.find_element_by_tag_name('a')
                            el_a.click()
                            break
            except:
                print("I am trying to download a file ...\n\n")
            return

    except Exception as ex:
        print(f"\n\n\n{ex}")


def select_calendar_date(fromDate, toDate):
    try:
        calendar_id = "initDate"
        el_calendar = driver.find_element_by_id(calendar_id)
        el_calendar.click()

        # Detect the elements values
        css_calendar = "ui-datepicker-calendar"

        try:  # Searching for the year
            years_select = driver.find_element_by_xpath(
                "//*[@id='ui-datepicker-div']/div/div/select[2]")
            years = years_select.find_elements_by_tag_name('option')
            for year in years:
                value = year.get_attribute('value')
                if(int(value) == fromDate.year):
                    year.click()
                    break
        except Exception as ex:
            print(f"Error to select the year form the calendar {ex}")

        # Searching for the month //*[@id="ui-datepicker-div"]/div/div/select[1]
        try:
            mont_select = driver.find_element_by_xpath(
                "//*[@id='ui-datepicker-div']/div/div/select[1]")
            months = mont_select.find_elements_by_tag_name('option')
            for month in months:
                value = month.get_attribute('value')
                if(int(value) + 1 == fromDate.month):
                    month.click()
                    break
        except Exception as ex:
            print(f"Error to select the month form the calendar {ex}")

        try:  # Searching for the day
            # loop to find the first date of the month
            el_calendar = driver.find_element_by_class_name(css_calendar)
            rows = el_calendar.find_elements_by_tag_name('tr')
            for x in rows:
                columns = x.find_elements_by_tag_name('td')
                for y in columns:
                    try:
                        y_string = y.text.strip()
                        if int(y_string) == fromDate.day:
                            el_a = y.find_element_by_tag_name('a')
                            el_a.click()
                            break
                    except:
                        pass
        except Exception as ex:
            print(f"Error selecting the day from the calendar {ex}")
    except Exception as ex:
        print(f"\n\n\n{ex}")


def get_dollar_exchange(dbs, servers):
    try:
        print("download_path")
        print(download_path)

        # Elementes that we are gonna need for the selenium bot
        # Frame exchange id: money-converter
        # Select tag exchange id: countryDropDownList
        # Select option tag exchange values child: Honduras, value: HN
        # Compra id ezchange: exchangeRateBuyUSD
        # Venta id exchange: exchangeRateSellUSD
        tablename = "exchanges"
        datatable = ""
        data = []
        headers_fields = []

        el_frame = "money-converter"
        el_selection = "countryDropDownList"
        el_selection_value_option_xpath = "//*[@id='countryDropDownList']/option[6]"
        el_RateBuy = "exchangeRateBuyUSD"
        el_RateSell = "exchangeRateSellUSD"

        se_buy = driver.find_element_by_id(el_RateBuy)
        se_sell = driver.find_element_by_id(el_RateSell)
        se_selection = driver.find_element_by_id(el_selection)
        se_option_selection = driver.find_element_by_xpath(
            el_selection_value_option_xpath)

        se_selection.click()
        se_option_selection.click()

        buy_text = float(se_buy.text.split('L')[1])
        sell_text = float(se_sell.text.split('L')[1])

        formated_date = datetime.now().date()
        headers_fields = ["CurrencyBase", "Sell", "Buy", "Date", "BankId"]

        data = [
            ["USD", sell_text, buy_text, str(formated_date), 1]
        ]

        for server in servers:
            for db in dbs:
                try:
                    database = db
                    SqlServer = db_server()
                    SqlServer.get_mysql_connection(
                        server["ServerName"], db["Name"], server["User"], server["Password"])

                    original_teble_name = SqlServer.get_original_table_name(
                        tablename)

                    query = f"SELECT Date from {original_teble_name} order by Date DESC Limit 1;"
                    last_transaction = SqlServer.get_data_query(query)
                    if(last_transaction == 0):
                        SqlServer.insert_rows(tablename, headers_fields, data)
                        continue

                    db_formated_date = datetime(
                        last_transaction[0].year, last_transaction[0].month, last_transaction[0].day)
                    new_formated_date = datetime(
                        formated_date.year, formated_date.month, formated_date.day)

                    if(db_formated_date == new_formated_date):
                        continue

                    SqlServer.insert_rows(tablename, headers_fields, data)
                    SqlServer.close_connection()

                except Exception as ex:
                    print("Some proble with the database")
                    print(ex)

    except Exception as ex:
        print(ex)
        print("The funciton get_dollar_exchange has fails")
    finally:
        print("method executed get_dollar_exchange \n")


def Init(credentailsPath, csvpath, dbs, servers):
    try:
        el_usernameID = "productId"
        el_passwordID = "pass"
        bt_SubmitFormID = "confirm"

        # 1 Verify there is no file named Transacciones del mes.csv
        os.system('cls')
        time.sleep(1)
        print("Spider bot initiated...\n\n")

        if os.path.exists(csvpath):
            os.remove(csvpath)

        # 2 Get credentials
        file = open(credentailsPath, mode='r')
        rows = file.readlines()
        username = rows[0]
        password = rows[1]

        # 3 Start to navegate in the browser
        print("we are going to the bac credomatic web-site")
        driver.get("https://www1.sucursalelectronica.com/redir/showLogin.go")
        get_dollar_exchange(dbs, servers)
        se_username = driver.find_element_by_id(el_usernameID)
        se_password = driver.find_element_by_id(el_passwordID)
        se_submit = driver.find_element_by_id(bt_SubmitFormID)
        se_username.send_keys(username)
        se_password.send_keys(password)
        se_submit.click()

        print("\nFuck warnings ...\n")
        el_sessions_page = []
        el_sessions_page = driver.find_elements_by_class_name(
            "principalTitles")
        if(len(el_sessions_page) > 0):
            el_buttons = driver.find_elements_by_name("back")
            if(len(el_buttons) > 0):
                el_buttons[0].click()

        find_and_download_transactions()
        print("Spider bot has complete...\n\n")

    except Exception as ex:
        print(ex)


if __name__ == "__main__":

    csvPath = "Transacciones del mes.csv"
    dbs = [
        {
            "Name": "db_finance"
        }
        # {
        #     "Name": "db_finance_dev"
        # }
    ]

    server_db = [
        {
            "ServerName": "",
            "User": "",
            "Password": ""
        },
        {
            "ServerName": "",
            "User": "",
            "Password": ""
        }
    ]

    os.system('clear')
    Init("cred.txt", csvPath, dbs, server_db)

    print("Saving the data....\n\n")
    for itemx in range(len(server_db)):
        print(
            f"\nProccesing the data for the server: {server_db[itemx]}\n{('--'  * 50)}")
        for db in dbs:
            database = db
            save_data_sqlserver(server_db[itemx]["ServerName"], db["Name"],
                                csvPath, server_db[itemx]["User"], server_db[itemx]["Password"])

    os.remove(csvPath)
    driver.close()
    exit(0)
