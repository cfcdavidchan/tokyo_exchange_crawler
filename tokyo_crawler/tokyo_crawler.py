from io import StringIO, BytesIO
from bs4 import BeautifulSoup
import requests, re
from urllib.parse import urljoin
import camelot
import os, csv, json
from pdfminer.converter import TextConverter
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
import urllib.request
from os import linesep

class JP_SecurityCrawler:
    def __init__(self, output_all: bool = True, reference_json_filename: str = ""):
        self.IPO_base_url = "https://www.jpx.co.jp/listing/stocks/new/"  # the base url from Tokyo exchange
        self.JP_exchange_base = "https://www.jpx.co.jp/"
        self.IPO_col = ["date of listing", "issue name", "code", "outline pdf url", "market segment", "prospectus pdf url", "share price", "issue_name (English name)",
                        "No. of Issued Shares", "No. of Issued Shares (incl. treasury shares)", "Managing Trading Participant", "Public Offering/Secondary Offering",
                        "Public Offering / Secondary Offering - (Placement Underwritten and Purchased by Principal Underwriting Participants)", "Original Share Offers",
                        "Shareholders – Holding stake"]
        self.searched_history = []
        self.output_all = output_all
        self.reference_json_filename = reference_json_filename
        if not self.output_all:
            #  Retrieving the searched IPO tickers
            self.searched_history = self.read_tickers_list(json_filename=self.reference_json_filename)

    @staticmethod
    def share_price_parser(share_price: str):
        """
        return float if it can be converted as float otherwise will return string itself
        :param share_price:
        :return:
        """
        try:
            share_price = re.sub('[\-,]', '', share_price)
            return float(share_price)
        except:
            return share_price

    def get_all_ipo(self):
        IPO_response = requests.get(url=self.IPO_base_url, verify=False)
        IPO_response.encoding = 'utf-8'  # converting to utf-8 to read japanese
        IPO_soup = BeautifulSoup(IPO_response.text, "html.parser")
        all_ticker_table = IPO_soup.find("div", {"class": "component-normal-table"}).find("tbody").findAll('tr')

        ipo_list = []
        for i in range(0, len(all_ticker_table), 2):
            ipo_row = all_ticker_table[i:i + 2]
            # reading the first row of the IPO table
            ipo_first_row = ipo_row[0].findAll('td')
            date = ipo_first_row[0].text
            date_of_listing = date[:date.find('（')].strip()  # finding the () then removing all the white space
            issue_name = ipo_first_row[1].text.strip()  # retrieving the issue name
            code = ipo_first_row[2].text.strip()  # retrieving the underlying code
            outline_pdf_url = ipo_first_row[3].find("a", href=True)['href']  # retrieving the relative path of the pdf file
            if outline_pdf_url:
                outline_pdf_url = urljoin(self.JP_exchange_base, outline_pdf_url)  # joining the path as the full url path
            # reading the second row of the IPO table
            ipo_second_row = ipo_row[1].findAll('td')
            market_segment = ipo_second_row[0].text.strip()  # retrieving the market segment
            prospectus_pdf_url = ipo_second_row[1].find("a", href=True)['href']  # retrieving the relative path of prospectus pdf
            if prospectus_pdf_url:
                prospectus_pdf_url = urljoin(self.JP_exchange_base, prospectus_pdf_url)  # joining the path as the full url path
            share_price = ipo_second_row[3].text.strip()  # retrieving the share price if it is existing
            share_price = JP_SecurityCrawler.share_price_parser(share_price)

            ipo_dict = {
                "date of listing": date_of_listing,
                "issue name": issue_name,
                "code": code,
                "outline pdf url": outline_pdf_url,
                'market segment': market_segment,
                "prospectus pdf url": prospectus_pdf_url,
                "share price": share_price
            }
            ipo_list.append(ipo_dict)
        return ipo_list

    def outline_pdf_parser(self, pdf_url):

        # all the default key, value
        outline_pdf_summary = {
            "issue_name (English name)": "",
            "No. of Issued Shares": "",
            "No. of Issued Shares (incl. treasury shares)": "",
            "Managing Trading Participant": "",
            "Public Offering/Secondary Offering": [],
            "Public Offering / Secondary Offering - (Placement Underwritten and Purchased by Principal Underwriting Participants)": "",
            "Original Share Offers": []
        }
        try:
            # Reading the pdf
            tables = camelot.read_pdf(pdf_url, pages='all', flavor='stream')
            for page_number, page in enumerate(tables):
                page_df = page.df
                # clean up all the unnecessary space in every row
                page_df[0] = page_df[0].apply(lambda x: re.sub('[\-,\n\t（）]', '', str(x)).strip().lower().replace(' ', '') if isinstance(x, str) else x)
                page_df[1] = page_df[1].apply(lambda x: re.sub('[\-,\n\t]', '', str(x)).strip().lower().replace(' ', '') if isinstance(x, str) else x)

                for index, row in page_df.iterrows():
                    if "英訳名" == row[0]:
                        outline_pdf_summary["issue_name (English name)"] = re.sub('[\-,\n\t（）]', '', row[1])
                    if '発行済株式総数' == row[0]:
                        outline_pdf_summary["No. of Issued Shares"] = row[1]
                    if '上場時発行済株式総数' == row[0]:
                        outline_pdf_summary["No. of Issued Shares (incl. treasury shares)"] = row[1]
                    if '幹事取引参加者' == row[0]:
                        outline_pdf_summary["Managing Trading Participant"] = row[1]
                    if '公募' in row[1]:
                        if re.findall("\d+", row[1]):
                            outline_pdf_summary["Public Offering/Secondary Offering"].append(row[1])
                    if '売出し（引受人の買取引受による売出し）' in row[1]:
                        clean_up_price = re.findall("\d+", row[1])
                        if clean_up_price:
                            outline_pdf_summary["Public Offering / Secondary Offering - (Placement Underwritten and Purchased by Principal Underwriting Participants)"] = clean_up_price[0]
                        else:
                            outline_pdf_summary["Public Offering / Secondary Offering - (Placement Underwritten and Purchased by Principal Underwriting Participants)"] = row[1]
                    if "売出株放出元" in row[0]:
                        outline_pdf_summary["Original Share Offers"].append(row[1])
                        while True:
                            next_row = page_df.loc[index + 1]
                            if not len(next_row[0]):
                                outline_pdf_summary["Original Share Offers"].append(next_row[1])
                                index += 1
                            else:
                                break
        except:
            pass
        return outline_pdf_summary

    def prospectus_pdf_parser_v2(self, pdf_url):
        def extract_text_by_page(pdf_path):
            """
            This function is to convert the entire pdf to text, will output per page with generator
            :param pdf_path:
            :return: generator
            """
            request = urllib.request.Request(pdf_path)

            response = urllib.request.urlopen(request).read()
            fb = BytesIO(response)

            #  converting the entire pdf to text base
            for page_number, page_object in enumerate(PDFPage.get_pages(fb, caching=True, check_extractable=True), 1):  # page number starting from 1
                resource_manager = PDFResourceManager()
                fake_file_handle = StringIO()
                converter = TextConverter(resource_manager, fake_file_handle)
                page_interpreter = PDFPageInterpreter(resource_manager, converter)
                page_interpreter.process_page(page_object)
                text = fake_file_handle.getvalue()
                yield page_number, text
                # close the handler when the page is read
                converter.close()
                fake_file_handle.close()

        prospectus_pdf_summary_dict = {
            "Shareholders – Holding stake": []
        }
        table_end = False  # a flag to break the for loop
        for page_number, page_text in extract_text_by_page(pdf_url):  # loop thought the pdf to text by generator function
            if "株主の状況" in page_text:  # find the key header word of the table
                tables = camelot.read_pdf(pdf_url, pages=f'{page_number}')
                for table in tables:
                    df = table.df
                    new_header = df.iloc[0]
                    df = df[1:]
                    df.columns = new_header
                    if "氏名又は名称" in df.columns:  # target table is found
                        for name, share in zip(df.iloc[:, 0].to_list(), df.iloc[:, 2].to_list()):
                            if "計" not in name:
                                prospectus_pdf_summary_dict["Shareholders – Holding stake"].append(f"{name.replace(linesep, '')} {share}")
                            else:
                                table_end = True
                                break
                        while not table_end:
                            page_number += 1
                            tables = camelot.read_pdf(pdf_url, pages=f'{page_number}')
                            df = tables[0].df
                            new_header = df.iloc[0]
                            df = df[1:]
                            df.columns = new_header
                            if "氏名又は名称" in df.columns:
                                for name, share, percent in zip(df.iloc[:, 0].to_list(), df.iloc[:, 2].to_list(), df.iloc[:, 3].to_list()):
                                    if ("計" in name) or ("100" in percent):
                                        table_end = True
                                        break
                                        # print('pending')
                                    else:
                                        prospectus_pdf_summary_dict["Shareholders – Holding stake"].append(f"{name.replace(linesep, '')} {share}")
            if table_end:  # break the for loop if the table is obtained
                break
        return prospectus_pdf_summary_dict

    def prospectus_pdf_parser(self, pdf_url):
        prospectus_pdf_summary_dict = {
            "Shareholders – Holding stake": []
        }
        try:
            shareholders_page_number = None
            handler = camelot.handlers.PDFHandler(pdf_url, pages="all")
            for page_num in handler.pages[::-1]:
                try:
                    tables = camelot.handlers.PDFHandler(pdf_url, pages=f"{page_num}").parse(flavor='stream', suppress_stdout=False)[0].df
                    tables_tittle = tables[0][0]
                    print(tables_tittle)
                    if "株主の状況" in tables_tittle:
                        shareholders_page_number = page_num
                        break
                except:
                    pass
            if shareholders_page_number:
                tables = camelot.read_pdf(pdf_url, pages=f"{shareholders_page_number}")
                df = tables[0].df
                for columns_num in df.columns:
                    df[columns_num] = df[columns_num].apply(lambda x: re.sub('[\-,\n\t]', '', str(x)).strip().lower().replace(' ', '') if isinstance(x, str) else x)
                df = df.drop(df.index[0])
                for index, row in df.iterrows():
                    if "計" in row[0]:
                        break
                    prospectus_pdf_summary_dict["Shareholders – Holding stake"].append(f"{row[0]} {row[2]}")
        except:
            pass
        return prospectus_pdf_summary_dict

    def read_tickers_list(self, json_filename: str):
        with open(json_filename) as json_file:
            data = json.load(json_file)
        return data

    def write_ticker_json(self, json_filename: str, data):
        with open(json_filename, 'w') as json_file:
            json.dump(data, json_file)

    def run(self, to_csv: bool = True, csv_filename: str = ""):
        all_ipo_list = self.get_all_ipo()
        IPO_result_list = []
        if not self.output_all:
            for ipo in all_ipo_list:
                ticker = ipo['issue name']
                share_price = ipo['share price']
                if not (ticker and share_price):
                    continue
                if ticker not in self.searched_history:
                    IPO_result_list.append(ipo)
                    self.searched_history.append(ticker)
        else:
            IPO_result_list = all_ipo_list

        for ipo in IPO_result_list:
            outline_pdf_url = ipo["outline pdf url"]
            prospectus_pdf_url = ipo["prospectus pdf url"]
            if outline_pdf_url:
                ipo.update(self.outline_pdf_parser(pdf_url=outline_pdf_url))
            if prospectus_pdf_url:
                ipo.update(self.prospectus_pdf_parser_v2(pdf_url=prospectus_pdf_url))

        # update searched_history json
        self.write_ticker_json(json_filename=self.reference_json_filename, data=self.searched_history)
        if to_csv:
            if not csv_filename:
                print('Please provide a csv filename')
            else:
                csv_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), csv_filename)
                with open(csv_filepath, 'w', encoding='utf-8-sig', newline="") as csv_file:
                    dict_writer = csv.DictWriter(csv_file, list(self.IPO_col))
                    dict_writer.writeheader()
                    dict_writer.writerows(IPO_result_list)

        return IPO_result_list


if __name__ == "__main__":
    crawler = JP_SecurityCrawler(output_all=False, reference_json_filename="searched_ipo.json")
    all_ipo = crawler.run(to_csv=True, csv_filename='ipo_test.csv')
    from pprint import pprint

    pprint(all_ipo)
    pass