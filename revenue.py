import asyncio
from playwright.async_api import async_playwright
import json
import re
import random
import os
from bs4 import BeautifulSoup
import csv

LISTORG_FAIL_COUNT = 0
MAX_LISTORG_FAILS = 3

async def fetch_from_listorg(page, inn):
    global LISTORG_FAIL_COUNT
    
    try:
        if LISTORG_FAIL_COUNT >= MAX_LISTORG_FAILS:
            return None, "skip"
        
        search_url = f"https://www.list-org.com/search?val={inn}&type=inn&sort="
        await page.goto(search_url, wait_until='domcontentloaded', timeout=10000)
        
        try:
            await asyncio.wait_for(
                page.wait_for_selector('.card.w-100.p-1.p-lg-3.mt-1, text="Проверка", text="403", text="Forbidden"', timeout=5000),
                timeout=5
            )
        except asyncio.TimeoutError:
            LISTORG_FAIL_COUNT += 1
            print(f"List-org не отвечает (провал #{LISTORG_FAIL_COUNT})")
            return None, "timeout"
        
        blocked_text = await page.inner_text('body')
        if any(x in blocked_text for x in ['Проверка', '403', 'Forbidden', 'Доступ ограничен']):
            LISTORG_FAIL_COUNT += 1
            print(f"List-org заблокировал (провал #{LISTORG_FAIL_COUNT})")
            return None, "blocked"
        
        card_selector = '.card.w-100.p-1.p-lg-3.mt-1'
        try:
            company_link = await page.query_selector(f'{card_selector} a')
            if not company_link:
                return None, "no_card"
        except:
            return None, "no_card"
        
        company_href = await company_link.get_attribute('href')
        company_url = f"https://www.list-org.com{company_href}" if company_href.startswith('/') else company_href
        
        await page.goto(company_url, wait_until='domcontentloaded', timeout=10000)
        await asyncio.sleep(random.uniform(0.5, 1))
        
        page_text = await page.inner_text('body')
        
        if LISTORG_FAIL_COUNT > 0:
            LISTORG_FAIL_COUNT = 0
        
        return page_text, "success"
        
    except Exception as e:
        LISTORG_FAIL_COUNT += 1
        print(f"List-org ошибка (провал #{LISTORG_FAIL_COUNT}): {str(e)[:60]}")
        return None, f"error"

async def fetch_from_rbc(page, inn):
    try:
        search_url = f"https://companies.rbc.ru/search/?query={inn}"
        await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(random.uniform(1, 2))
        
        if await page.query_selector('text="ничего не найдено"') or await page.query_selector('text="Не найдено"'):
            print(f"RBC: ничего не найдено для ИНН {inn}")
            return None, "not_found"
        
        company_link = await page.query_selector('.company-card.info-card a') or await page.query_selector('.search-results-item a')
        if not company_link:
            return None, "no_card"
        
        company_href = await company_link.get_attribute('href')
        if not company_href.startswith('http'):
            company_href = f"https://companies.rbc.ru{company_href}"
        
        await page.goto(company_href, wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(random.uniform(1, 2))
        
        html_content = await page.content()
        
        return html_content, "success"
        
    except Exception as e:
        print(f"RBC ошибка: {str(e)[:80]}")
        return None, f"error"

def extract_financial_data(html_content, inn, company_name, companies):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    of_name = ""
    h1_tag = soup.find('h1')
    if h1_tag:
        of_name = h1_tag.get_text(strip=True)
    
    all_text = soup.get_text(separator=' ', strip=False)
    
    pattern1 = r'выручк[а-я]*\s+за\s+(\d{4})\s+год\s*[—\-]\s*([\d\s,\.]+)\s*₽'
    match = re.search(pattern1, all_text, re.IGNORECASE)
    
    if not match:
        pattern2 = r'([\d\s,\.]+)\s*₽\s+за\s+(\d{4})\s+год'
        match = re.search(pattern2, all_text, re.IGNORECASE)
    
    if not match:
        pattern3 = r'За\s+(\d{4})\s+год[^₽]*выручк[а-я]*[^₽]*([\d\s,\.]+)\s*₽'
        match = re.search(pattern3, all_text, re.IGNORECASE)
    
    if not match:
        lines = all_text.split('\n')
        for line in lines:
            if 'выруч' in line.lower() and '₽' in line:
                year_match = re.search(r'(\d{4})', line)
                if year_match:
                    amount_match = re.search(r'([\d\s,\.]+)\s*₽', line)
                    if amount_match:
                        revenue_year = year_match.group(1)
                        revenue_str = amount_match.group(1)
                        match = True
                        break
    
    result_data = {"of_name": of_name}
    
    if match:
        if 'match' in locals() and match:
            groups = match.groups()
            
            if len(groups) == 2:
                for g in groups:
                    if re.match(r'^\d{4}$', str(g)):
                        revenue_year = g
                    else:
                        revenue_str = g
                
                cleaned_str = revenue_str.replace(' ', '').replace(',', '.').replace('\xa0', '')
                cleaned_str = re.sub(r'[^\d.]', '', cleaned_str)
                
                try:
                    revenue = int(float(cleaned_str)) if '.' in cleaned_str else int(cleaned_str)
                    
                    result_data["revenue_year"] = revenue_year
                    result_data["revenue"] = revenue
                    
                    for company in companies:
                        if company.get("inn") == inn:
                            company["of_name"] = of_name
                            company["revenue_year"] = revenue_year
                            company["revenue"] = revenue
                            break
                    
                    print(f"Найдено: {revenue_year} год — {revenue:,} руб.")
                    return result_data
                    
                except ValueError:
                    print(f"Не удалось преобразовать число: {revenue_str}")
    
    print(f"Выручка не найдена")
    
    if of_name:
        for company in companies:
            if company.get("inn") == inn:
                company["of_name"] = of_name
                break
    
    return result_data if of_name else None

async def process_company(browser, inn, company_name, companies, semaphore):
    async with semaphore:
        page = await browser.new_page()
        
        try:
            source = None
            page_text = None
            status = None
            
            if LISTORG_FAIL_COUNT < MAX_LISTORG_FAILS:
                page_text, status = await fetch_from_listorg(page, inn)
                source = "list-org"
            
            if not page_text or status in ["blocked", "timeout", "error", "skip"]:
                await page.close()
                page = await browser.new_page()
                
                page_text, status = await fetch_from_rbc(page, inn)
                source = "rbc"
            
            if not page_text:
                await page.close()
                return inn, None, source
            
            result = extract_financial_data(page_text, inn, company_name, companies)
            
            await page.close()
            return inn, result, source
            
        except Exception as e:
            print(f"Ошибка для ИНН {inn}: {str(e)[:60]}")
            try:
                await page.close()
            except:
                pass
            return inn, None, "error"

def safe_int_convert(value):
    if not value:
        return 0
    cleaned_str = re.sub(r'[^\d.]', '', str(value))
    try:
        return int(float(cleaned_str))
    except (ValueError, TypeError):
        return 0

def save_to_csv(companies):
    try:
        filtered_companies = [
            company for company in companies 
            if safe_int_convert(company.get("revenue")) > 200000000
        ]
        
        print(f"Найдено {len(filtered_companies)} компаний с выручкой > 200 млн руб.")
        
        if not filtered_companies:
            print("Нет данных для сохранения")
            return
        
        fieldnames = [
            'name', 'of_name', 'inn', 'revenue_year', 'revenue',
            'segment_tag', 'contacts', 'email', 'site', 'rating_PPAP', 'source'
        ]
        
        descriptions = {
            'name': 'Название из alladvertising',
            'of_name': 'Официальное название компании',
            'inn': 'ИНН',
            'revenue_year': 'Год выручки',
            'revenue': 'Сумма выручки (руб.)',
            'segment_tag': 'Сегмент (AGENCY/MEDIA/OTHER)',
            'contacts': 'Контактный телефон',
            'email': 'Email',
            'site': 'Сайт',
            'rating_PPAP': 'Рейтинг PPAP',
            'source': 'Источники данных: alladvertising.ru, поисковые системы (Google, Yandex, Yahoo), companies.rbc.ru, list-org.com'
        }
        
        with open('companies_export.csv', 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, delimiter=';')
            
            writer.writerow(fieldnames)
            
            writer.writerow([descriptions.get(field, '') for field in fieldnames])
            
            for idx, company in enumerate(filtered_companies, start=1):
                row = []
                for field in fieldnames:
                    if field == 'source':
                        row.append(descriptions['source'])
                    else:
                        value = company.get(field, '')
                        if field == 'revenue':
                            value = safe_int_convert(value)
                        row.append(str(value))
                writer.writerow(row)
        
        print(f"Данные сохранены в companies_export.csv")

    except Exception as e:
        print(f"Произошла ошибка при сохранении в CSV: {e}")

async def main():
    global LISTORG_FAIL_COUNT
    
    input_file = 'companies_stage1.json'
    if not os.path.exists(input_file):
        print(f"Файл {input_file} не найден!")
        return
    
    with open(input_file, 'r', encoding='utf-8') as f:
        companies = json.load(f)
    
    companies_to_process = []
    name_by_inn = {}
    
    for company in companies:
        inn = company.get("inn", "").strip()
        if inn and not company.get("revenue") and not company.get("revenue_year"):
            companies_to_process.append(inn)
            name_by_inn[inn] = company["name"]
    
    print(f"Компаний для обработки: {len(companies_to_process)}")
    
    if companies_to_process:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            
            semaphore = asyncio.Semaphore(5)
            
            processed = 0
            failed = 0
            listorg_success = 0
            rbc_success = 0
            
            tasks = []
            for inn in companies_to_process:
                company_name = name_by_inn.get(inn, inn)
                task = process_company(browser, inn, company_name, companies, semaphore)
                tasks.append(task)
            
            print(f"Начинаем обработку...")
            print(f"Текущий счётчик провалов list-org: {LISTORG_FAIL_COUNT}")
            
            for i, task in enumerate(asyncio.as_completed(tasks)):
                inn, result, source = await task
                name = name_by_inn.get(inn, inn)
                
                if result:
                    processed += 1
                    if source == "list-org":
                        listorg_success += 1
                    elif source == "rbc":
                        rbc_success += 1
                    
                    revenue_str = f"{result.get('revenue', 0):,}" if result.get('revenue') else "нет"
                    year_str = result.get('revenue_year', 'не найден')
                    of_name = result.get('of_name', 'не найден')
                    print(f"[{i+1}/{len(tasks)}] {name[:25]:25} | год: {year_str:4} | revenue: {revenue_str:>15} | of_name: {of_name[:30]}")
                else:
                    failed += 1
                    print(f"[{i+1}/{len(tasks)}] {name[:25]:25} | не найдено")
                
                if LISTORG_FAIL_COUNT >= MAX_LISTORG_FAILS:
                    print(f"LIST-ORG ЗАБЛОКИРОВАН! Используем только RBC")
            
            await browser.close()
        
        print(f"\nИТОГИ ОБРАБОТКИ ВЫРУЧКИ:")
        print(f"Успешно: {processed} компаний")
        print(f"Из них с list-org: {listorg_success}")
        print(f"Из них с RBC: {rbc_success}")
        print(f"Не удалось: {failed} компаний")
        print(f"Провалов list-org: {LISTORG_FAIL_COUNT}")
        
        with open('companies_stage1.json', 'w', encoding='utf-8') as f:
            json.dump(companies, f, ensure_ascii=False, indent=2)
        print(f"Данные обновлены в companies_stage1.json")
    else:
        print("Все компании уже обработаны")
    
    print("\nСОХРАНЕНИЕ В CSV...")
    save_to_csv(companies)
    print("РАБОТА ЗАВЕРШЕНА!")


if __name__ == "__main__":
    asyncio.run(main())