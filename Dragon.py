
import os
import requests
from bs4 import BeautifulSoup
from time import sleep
import random
# from database import insert_fund_tailieu, connect_db_sub
import re
# from sqlalchemy import create_engine, text
import time
import random

class FundDownloader:
    def __init__(self, engine = None):
        self.url = (
            "https://www.dragoncapital.com.vn/individual/vi/webruntime/api/apex/"
            "execute?language=vi&asGuest=true&htmlEncode=false"
        )
        self.headers = {
            "content-type": "application/json; charset=utf-8",
            "cookie": "LSKey-c$forcedLandingVi=true; LSKey-c$savedUserLang=vi; _ga=GA1.3.1701455539.1712049822; _ga_JFSL1NFDB6=GS1.1.1722996358.5.0.1722996358.60.0.0; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1; guest_uuid_essential_0DMJ2000000oLuf=f21825c0-1b8b-4cf8-8542-b8298285f05b; guest_uuid_essential_0DMJ2000000oLup=2a0e2e45-cc1f-4dcf-8836-a554b5d1772f; guest_uuid_essential_0DMJ2000000oLuk=f117bdb4-66ee-4cb2-8082-2152b81dfd13; sfdc-stream=\u00213hfSHL8jUREuzeyfvtl8yvPZaE2DWjgXjgfCbiIF+aUx5xd3SIa75A8jL2jCSDZo7bQFcyt2ZIEeRrU=",  # Cookie của bạn
            "origin": "https://www.dragoncapital.com.vn",
            "priority": "u=1, i",
            "referer": "https://www.dragoncapital.com.vn/individual/vi/report",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0"
            ),
        }
        self.engine = engine
    
    def fetch_fund_codes(self):
        """
        Lấy danh sách fund_code + fundReportCode__c từ API
        Tham khảo logic code cũ:
        - @udd/01pJ2000000CgSu => method="getAvailableFunds"
        """
        payload = {
            "namespace": "",
            "classname": "@udd/01pJ2000000CgSu",
            "method": "getAvailableFunds",
            "isContinuation": False,
            "params": {
                "siteId": "0DMJ2000000oLukOAE",
            },
            "language": "vi",
            "asGuest": True,
            "cacheable": True,
            "htmlEncode": False
        }
        try:
            resp = requests.post(self.url, headers=self.headers, json=payload)
            resp.raise_for_status() # Throw new error if response code is 4xx or 5xx
        except requests.HTTPError as e:
            print(f"fetch_fund_codes -> request error: {e}")
            return []

        data_json = resp.json() # Convert string to JSON format
        return_value = data_json.get("returnValue", [])  # It will return an empty array if "returnValue" is undefined
        if not return_value:
            return []
        codes = []
        for item in return_value:
            fund_report_code = item.get("fundReportCode__c")
            fund_code = item.get("fundCode__c")
            if fund_report_code and fund_code:
                codes.append((fund_report_code, fund_code))
        return codes
    
    def fetch_links(self, code: str, year: int, keyword: str = ''):
        payload = {
            "namespace": "",
            "classname": "@udd/01pJ2000000CgR7",
            "method": "getDocumentContentsV2",
            "isContinuation": False,
            "params": {
                "siteId": "0DMJ2000000oLukOAE",
                "fundCodeOrReportCode": code,
                "documentType": "FUND_DISCLOSURE",
                "targetYear": str(year),
                "language": "vi"
            },
            "cacheable": False
        }
        try:
            resp = requests.post(self.url, headers=self.headers, json=payload)
            resp.raise_for_status()
        except Exception as e:
            print(f"Error fetching links for code={code}, year={year}: {e}")
            return []

        data = resp.json()
        return_value = data.get("returnValue", [])
        if not return_value:
            return []

        files_info = return_value[0].get("files", [])
        links = []
        for item in files_info:
            file_name = item.get("activeFileName__c", "")

            if keyword:
                if keyword in file_name.lower():
                    href = item.get("downloadUrl__c", "")
                    if href:
                        links.append((file_name, href))
                        break
            else:
                href = item.get("downloadUrl__c", "")
                links.append((file_name, href))
                break

        print("Links: ", links)
        return links
    
    def download(self, fund_code: str, code: str, year: int):
        """
        - Tạo thư mục
        - Fetch link
        - Download file
        - Lưu meta vào DB với insert_fund_tailieu (ON CONFLICT DO NOTHING)
        """
        # Thư mục tương đối để lưu (ví dụ: ./data/nav)
        relative_folder = os.path.join('data', 'nav')

        # Tạo thư mục nếu chưa có
        os.makedirs(relative_folder, exist_ok=True)

        links = self.fetch_links(code, year)
        if not links:
            print(f"Không tìm thấy link cho fund_code={fund_code}, code={code}")
            return

        for filename, href in links:
            # Parse month
            match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", filename.lower())
            if match:
                # day = int(match.group(1))
                month = int(match.group(2))
                year = int(match.group(3))
                print("Tháng: ", month, ", year: ", year)
            else:
                month = 0  #default
                year = 0 
                print("Tháng: ", month, ", year: ", year)

            # Đường dẫn đầy đủ để lưu file
            savedFileName = os.path.basename(href)
            savedFileName = savedFileName.replace('/', '-')
            file_path = os.getcwd() + '\\' + relative_folder + '\\' + savedFileName

            # Tải và lưu file
            response = requests.get(href)

            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                print(f'✅ File đã lưu tại: {file_path}')
            else:
                print(f'❌ Không thể tải file. Mã lỗi: {response.status_code}')

def main():
    # engine = connect_db_sub()
    # downloader = FundDownloader(engine)
    downloader = FundDownloader()
    fund_list = [
        ('DCDS', 'VF1'), 
        ('DCBF', 'VFB'), 
        ('DCIP', 'VFC'), 
        ('DCDE', 'VF4'), 
        ('E1VFVN30', 'VFMVN30'), 
        ('FUEVFVND', 'VFMVND'), 
        ('VFMMID', 'VFMMID'), 
        ('VFMVSF', 'VEI')
    ]
    
    for year in range(2025, 2026):
        for fund_code, code in fund_list:
            downloader.download(fund_code, code, year)
            time.sleep(random.uniform(1, 3))
    print("=== Hoàn tất ===")
if __name__ == "__main__":
    main()


