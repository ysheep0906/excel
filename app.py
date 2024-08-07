def output_ul():
    import requests, os
    from bs4 import BeautifulSoup

    if os.path.exists('output.txt'):
        os.remove('output.txt')

    # 대상 웹페이지 URL
    url = 'https://www.coupang.com'  # 대상 URL로 변경하세요

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.coupang.com/'
        }
    # 웹페이지 요청
    response = requests.get(url, headers=headers)

    # 응답의 텍스트 (HTML) 파싱
    soup = BeautifulSoup(response.text, 'html.parser')

    # 셀렉터로 요소 찾기
    element = soup.select_one('#gnbAnalytics > ul.menu.shopping-menu-list')

    # 요소의 태그 복사
    if element:
        element_html = str(element)
        print(element_html)
        with open('output.txt', 'w', encoding='utf-8') as file:
            file.write(element_html)
    else:
        print("요소를 찾을 수 없습니다.")

def output_link():
    import re, os

    if os.path.exists('category_link.txt'):
        os.remove('category_link.txt')

    file_path = 'output.txt'
    output_file_path = 'category_link.txt'

    # 파일 읽기
    with open(file_path, 'r', encoding='utf-8') as file:
        text = file.read()

    matches = re.findall(r'"/np/categories/[^"]*"', text)

    with open(output_file_path, 'w', encoding='utf-8') as output_file:
        for match in matches:
            clean_match = match.strip('"')
            output_file.write(clean_match + '\n')

def output_product_index():
    import concurrent.futures
    import requests, os
    from bs4 import BeautifulSoup

    def fetch_product_indexes(url):
        base_url = 'https://www.coupang.com'
        full_url = base_url + url + '?listSize=120&sorter=latestAsc'

        #print(full_url)
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.coupang.com/'
        }

        response = requests.get(full_url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        product_list_element = soup.find('ul', id='productList')
        li_elements = product_list_element.find_all('li')

        results = []
        for li in li_elements:
            a_tag = li.find('a', class_='baby-product-link')
            if a_tag:
                itemid = a_tag.get('data-item-id')
                vendorid = a_tag.get('data-vendor-item-id')
                productid = a_tag.get('data-product-id')

            results.append((productid, itemid, vendorid))
        with open('indexes.txt', 'a') as file:
            for productid, itemid, vendorid in results:
                file.write(f'{productid},{itemid},{vendorid}\n')
        print(f'{url} 완료')

    if os.path.exists('indexes.txt'):
        os.remove('indexes.txt')

    with open('category_link.txt', 'r') as file:
        start_links = [line.strip() for line in file]

    # ThreadPoolExecutor를 사용하여 병렬 처리
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(fetch_product_indexes, start_links)

def output_dup_index():
    import os
    def remove_duplicates(input_file, output_file):
        unique_indexes = set()

        # 파일에서 중복을 제거할 데이터 읽어오기
        with open(input_file, 'r', encoding='utf-8') as file:
            for line in file:
                index = line.strip()
                if index:
                    unique_indexes.add(index)

        # 유니크한 데이터를 파일에 쓰기
        with open(output_file, 'w', encoding='utf-8') as file:
            for index in unique_indexes:
                file.write(index + '\n')

    if os.path.exists('unique_indexes.txt'):
        os.remove('unique_indexes.txt')

    input_file = 'indexes.txt'
    output_file = 'unique_indexes.txt'


    remove_duplicates(input_file, output_file)
    print("중복 제거 완료")

def crawl():
    import requests
    from bs4 import BeautifulSoup
    import os, json
    from datetime import datetime
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading, time

    results = []

    def parse_info(rows, full_url):
        data = {}
        
        for row in rows:
            th = row.find('th').get_text(strip=True)
            td = row.find('td').get_text(strip=True)
            
            if '상호/대표자' in th:
                parts = td.split('/')
                data['상호'] = parts[0].strip()
                data['대표자'] = parts[1].strip() if len(parts) > 1 else ''
            elif '사업장소재지' in th:
                data['사업장 소재지'] = td
            elif 'E-mail' in th:
                data['E-mail'] = td
            elif '연락처' in th:
                data['연락처'] = td
            elif '통신판매 신고번호' in th:
                data['통신판매 신고번호'] = td
            elif '사업자번호' in th:
                data['사업자번호'] = td
        data['URL'] = full_url
        print(data)
        return data

    def fetch_db(productid, itemid, vendorid):
        full_url = 'https://m.coupang.com/vm/products/' + itemid + '/brand-sdp/items/21375465444/seller-info/show-more?vendorItemId=' + vendorid
        
        excel_url = '/vp/products/' + productid + '?itemId=' + itemid + '&vendorItemId=' + vendorid

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.coupang.com/'
        }
        
        try:
            response = requests.get(full_url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            tbody_element = soup.find_all('table', class_='return-policy-table')
            rows = tbody_element[-1].find_all('tr')

            if len(rows) > 1:
                    batch_size = 1000
                    print(len(results))
                    if len(results) >= batch_size and len(results) <= batch_size + 5:
                        time.sleep(3)
                        save_results(results)
                        results.clear()
                    return parse_info(rows, excel_url)
            else:
                print(full_url + ' 크롤링 불필요 (tbody에 tr이 한 개)')
        except:
            print(full_url + '오류!!')

    def remove_duplicates(input_file, output_file):
        if os.path.exists(output_file):
            os.remove(output_file)

        seen = set()
        with open(input_file, 'r', encoding='utf-8') as infile:
            with open(output_file, 'w', encoding='utf-8') as outfile:
                for line in infile:
                    line_data = json.loads(line.strip())
                    if line_data and 'url' in line_data and line_data['url'] not in seen:
                        seen.add(line_data['url'])
                        outfile.write(json.dumps(line_data, ensure_ascii=False) + '\n')

    lock = threading.Lock()

    def save_results(data_batch):
            with lock:
                with open('results.txt', 'a', encoding='utf-8') as f:
                    for data in data_batch:
                        f.write(json.dumps(data, ensure_ascii=False) + '\n')

    def process_url(url):
        productid, itemid, vendorid = url.split(',')
        results.append(fetch_db(productid, itemid, vendorid))
    
    if os.path.exists('results.txt'):
        os.remove('results.txt')
        
    with open('unique_indexes.txt', 'r') as file:
        start_links = [line.strip() for line in file if line.strip()]

    # 병렬 크롤링 설정
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(process_url, url): url for url in start_links}
        
    # 남은 데이터를 저장
    if results:
        save_results(results)

    #results.txt 중복제거
    remove_duplicates('results.txt', 'unique_results.txt')

    # 결과 파일을 읽어서 엑셀로 변환
    all_data = []
    with open('unique_results.txt', 'r', encoding='utf-8') as f:
        for line in f:
            all_data.append(json.loads(line.strip()))

    df = pd.DataFrame(all_data)
    current_date = datetime.now().strftime('%Y-%m-%d')
    df.to_excel(f'쿠팡DB_{current_date}_url.xlsx', index=False)

def main():
    output_ul()

    output_link()

    output_product_index()

    output_dup_index()

    crawl()

if __name__  == "__main__":
    main()
