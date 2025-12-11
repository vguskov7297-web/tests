import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import re
import random
import time
import os
from playwright_stealth import stealth_async
import importlib.util

os.makedirs("debug", exist_ok=True)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
]

SEARCH_ENGINES = [
    {
        'name': 'Google',
        'url': 'https://www.google.com/',
        'input_selector': 'textarea[name="q"], input[name="q"]',
        'result_selector': 'div#search, div.g, .tF2Cxc',
        'timeout': 5000 
    },
    {
        'name': 'Yandex', 
        'url': 'https://ya.ru/search/',
        'input_selector': 'input[name="text"], .search3__input',
        'result_selector': '.serp-item, .Organic, .serp-list',
        'timeout': 7500,
        'handle_popup': True
    },
    {
        'name': 'Yahoo',
        'url': 'https://ru.search.yahoo.com/',
        'input_selector': 'input[name="p"], #yschsp',
        'result_selector': '.algo, .dd, .compTitle',
        'timeout': 7500
    },
]

async def scrape_company_details(page, detail_url):
    try:
        await page.goto(detail_url, wait_until='domcontentloaded', timeout=15000)

        html_content = await page.content()
        soup = BeautifulSoup(html_content, 'html.parser')

        rating_PPAP = ""
        rating_td = soup.find('td', class_='f14i')
        if rating_td:
            rating_text = rating_td.get_text(strip=True)
            match = re.search(r'(\d+)', rating_text)
            if match:
                rating_PPAP = match.group(1)

        phone = ""
        email = ""
        site = ""

        content_div = soup.find('div', id='content')
        if content_div:
            phone_text = content_div.get_text()
            phone_pattern = r'\+7\s*\(\d{3}\)\s*\d{3}-\d{2}-\d{2}|\+7\s*\d{3}\s*\d{3}-\d{2}-\d{2}'
            match = re.search(phone_pattern, phone_text)
            if match:
                phone = match.group(0)

            email_pattern = r'E-mail:\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
            match = re.search(email_pattern, phone_text)
            if match:
                email = match.group(1)

            site_link = content_div.find('a', rel='nofollow')
            if site_link and site_link.get('href'):
                href = site_link['href']
                if href.startswith('http://') or href.startswith('https://'):
                    site = href

        return {
            "contacts": phone,
            "email": email,
            "site": site,
            "rating_PPAP": rating_PPAP
        }

    except Exception as e:
        print(f"Ошибка при обработке страницы {detail_url}: {e}")
        return {"contacts": "", "email": "", "site": "", "rating_PPAP": ""}

async def scrape_alladvertising(base_url):
    companies_data = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(base_url)
        await page.wait_for_selector(".company", timeout=15000)

        company_elements = await page.query_selector_all(".company")
        print(f"Найдено {len(company_elements)} компаний на {base_url}")

        tasks = []
        for element in company_elements:
            try:
                name_element = await element.query_selector("h2")
                full_name = await name_element.inner_text() if name_element else "Неизвестно"

                name_parts = full_name.split('/')
                company_name = name_parts[0].strip() if name_parts else full_name
                segment_tag = name_parts[1].strip() if len(name_parts) > 1 else "OTHER"

                link_element = await element.query_selector("h2 a")
                detail_url = await link_element.get_attribute('href') if link_element else ""
                if not detail_url.startswith('http'):
                    detail_url = f"https://www.alladvertising.ru{detail_url}"

                tasks.append((company_name, segment_tag, detail_url))

            except Exception as e:
                print(f"Ошибка при извлечении данных для одной из компаний: {e}")
                continue

        print(f"Всего компаний на странице: {len(tasks)}")
        
        if not tasks:
            print(f"Нет компаний на {base_url}")
            await browser.close()
            return []

        existing_data = []
        if os.path.exists('companies_stage1.json'):
            try:
                with open('companies_stage1.json', 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except:
                existing_data = []
        
        existing_names = {company["name"] for company in existing_data}
        
        new_tasks = []
        for company_name, segment_tag, detail_url in tasks:
            if company_name not in existing_names:
                new_tasks.append((company_name, segment_tag, detail_url))
            else:
                print(f"Пропуск '{company_name}' (уже есть в файле)")
        
        print(f"Новых компаний для обработки: {len(new_tasks)} из {len(tasks)}")
        
        if not new_tasks:
            print(f"Нет новых компаний на {base_url}")
            await browser.close()
            return []

        processed_count = 0
        for company_name, segment_tag, detail_url in new_tasks:
            try:
                await asyncio.sleep(random.uniform(0.5, 1.5))
                print(f"Обрабатываем компанию: '{company_name}'")
                
                details = await scrape_company_details(page, detail_url)
                
                company_info = {
                    "name": company_name,
                    "segment_tag": segment_tag,
                    "inn": "",
                    "contacts": details["contacts"],
                    "email": details["email"],
                    "site": details["site"],
                    "rating_PPAP": details["rating_PPAP"],
                    "detail_url": detail_url
                }
                companies_data.append(company_info)
                processed_count += 1

                try:
                    if os.path.exists('companies_stage1.json'):
                        with open('companies_stage1.json', 'r', encoding='utf-8') as f:
                            try:
                                all_companies = json.load(f)
                            except:
                                all_companies = []
                    else:
                        all_companies = []
                    
                    all_companies.append(company_info)
                    
                    with open('companies_stage1.json', 'w', encoding='utf-8') as f:
                        json.dump(all_companies, f, ensure_ascii=False, indent=4)
                    
                    print(f"Сохранено ({processed_count}/{len(new_tasks)})")
                    
                except Exception as e:
                    print(f"Ошибка при сохранении: {e}")
                    continue

            except Exception as e:
                print(f"Ошибка при обработке компании '{company_name}': {e}")
                continue

        print(f"{base_url}: Успешно обработано {processed_count} из {len(new_tasks)} новых компаний.")

        await browser.close()

    return companies_data

async def find_inn_in_search_engine(browser, company_name, engine_info):
    page = await browser.new_page()
    await stealth_async(page)
    
    try:
        print(f"Пробуем {engine_info['name']}...")
        
        await page.goto(engine_info['url'], timeout=engine_info['timeout'])
        await asyncio.sleep(random.uniform(0.5, 1))
        
        if engine_info['name'] == 'Yandex':
            await page.mouse.click(50, 50)
            await asyncio.sleep(0.25)
        
        search_input = None
        for selector in engine_info['input_selector'].split(', '):
            try:
                search_input = await page.wait_for_selector(selector.strip(), 
                                                          state='visible', 
                                                          timeout=1500)
                if search_input:
                    break
            except:
                continue
        
        if not search_input:
            print(f"{engine_info['name']}: не нашел поле ввода")
            await page.close()
            return None
        
        await search_input.click()
        await asyncio.sleep(0.15)
        
        await search_input.fill('')
        await asyncio.sleep(0.1)
        
        query = f'рекламная компания {company_name} "ИНН"'
        await search_input.type(query, delay=random.randint(20, 50))
        await asyncio.sleep(0.25)
        
        await page.keyboard.press('Enter')
        await asyncio.sleep(random.uniform(1, 1.5))
        
        try:
            for selector in engine_info['result_selector'].split(', '):
                try:
                    await page.wait_for_selector(selector.strip(), 
                                               state='visible', 
                                               timeout=4000)
                    break
                except:
                    continue
            await asyncio.sleep(0.5)
        except:
            pass
        
        page_text = await page.inner_text('body')
        
        inn_match = re.search(r'ИНН\s*[:\-]?\s*(\d{10}|\d{12})', page_text, re.IGNORECASE)
        if inn_match:
            inn_found = inn_match.group(1)
            print(f"{engine_info['name']} нашел ИНН: {inn_found}")
            await page.close()
            return inn_found
        
        print(f"{engine_info['name']}: ИНН не найден")
        await page.close()
        return None
        
    except Exception as e:
        print(f"{engine_info['name']} ошибка: {str(e)[:100]}")
        await page.close()
        return None
    
async def find_inn_via_search_engines(browser, company_name):
    print(f"Поиск ИНН для '{company_name}'...")
    
    for engine in SEARCH_ENGINES:
        inn_result = await find_inn_in_search_engine(browser, company_name, engine)
        if inn_result:
            return inn_result
        await asyncio.sleep(random.uniform(0.5, 1))
    
    print(f"ИНН не найден ни в одном поисковике")
    return ""

async def get_and_save_company_list():
    print("ЭТАП 1: Сбор списка компаний и парсинг деталей...")
    
    print("Парсим Москву")
    moscow_companies = await scrape_alladvertising("https://www.alladvertising.ru/moscow/")
    
    print("Парсим Санкт-Петербург")
    spb_companies = await scrape_alladvertising("https://www.alladvertising.ru/spb/")

    all_companies = moscow_companies + spb_companies
    
    print(f"ИТОГИ ПАРСИНГА СПИСКА И ДЕТАЛЕЙ:")
    print(f"Москва: {len(moscow_companies)} компаний")
    print(f"Санкт-Петербург: {len(spb_companies)} компаний")
    print(f"Всего: {len(all_companies)} компаний")

    return all_companies

async def process_inn_for_all_companies():
    print("ЭТАП 2: Параллельный поиск ИНН...")
    
    if not os.path.exists('companies_stage1.json'):
        print("Ошибка: Файл 'companies_stage1.json' не найден.")
        return []

    with open('companies_stage1.json', 'r', encoding='utf-8') as f:
        companies = json.load(f)
    
    print(f"Загружено {len(companies)} компаний для поиска ИНН")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, args=['--start-maximized'])
        
        search_semaphore = asyncio.Semaphore(3)
        
        async def process_single_company(company, idx):
            if company.get("inn", "").strip():
                print(f"Пропуск компании {idx}/{len(companies)}: '{company['name']}' (ИНН уже есть)")
                return company
            
            print(f"Компания {idx}/{len(companies)}: '{company['name']}'")
            
            try:
                async with search_semaphore:
                    inn_result = await find_inn_via_search_engines(browser, company['name'])
                
                if inn_result:
                    company["inn"] = inn_result
                    print(f"ИНН найден: {inn_result}")
                    
                    companies[idx-1] = company
                    with open('companies_stage1.json', 'w', encoding='utf-8') as f:
                        json.dump(companies, f, ensure_ascii=False, indent=4)
                    print(f"Данные обновлены в JSON")
                else:
                    print(f"ИНН не найден")
                
                return company
                
            except Exception as e:
                print(f"Ошибка при обработке компании '{company['name']}': {e}")
                return company
        
        tasks = []
        for i, company in enumerate(companies, 1):
            if not company.get("inn", "").strip():
                task = process_single_company(company, i)
                tasks.append(task)
                await asyncio.sleep(0.1)
            else:
                tasks.append(asyncio.sleep(0))
        
        print(f"Запускаем параллельный поиск ИНН для {len(tasks)} компаний...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        found_inn = 0
        already_had_inn = sum(1 for c in companies if c.get("inn", "").strip())
        
        for result in results:
            if isinstance(result, dict) and result.get("inn"):
                found_inn += 1
        
        print(f"ИТОГИ ПОИСКА ИНН:")
        print(f"Всего компаний: {len(companies)}")
        print(f"Уже имели ИНН: {already_had_inn}")
        print(f"Найдено новых ИНН: {found_inn}")
        print(f"Осталось без ИНН: {len(companies) - already_had_inn - found_inn}")
        
        await browser.close()
    
    return companies

async def main():
    print(" ЗАПУСК ПАРСЕРА ALLADVERTISING.RU")
    
    print("\n Начинаем сбор компаний...")
    all_companies = await get_and_save_company_list()
    
    if all_companies:
        await process_inn_for_all_companies()
    else:
        print("Нет новых компаний для поиска ИНН")
    
    print("ЭТАП 1 ЗАВЕРШЕН! НАЧИНАЕМ ЭТАП 2...")
    print("============================================================")
    
    print("\nЗапуск получения финансовых данных...")
    
    try:
        # Загружаем модуль revenue
        spec = importlib.util.spec_from_file_location("revenue", "revenue.py")
        revenue_module = importlib.util.module_from_spec(spec)
        
        # Исполняем модуль, чтобы его функции стали доступны
        spec.loader.exec_module(revenue_module)
        
        # Правильно запускаем его main функцию с помощью await
        await revenue_module.main()
        
    except FileNotFoundError:
        print("Файл revenue.py не найден")
    except Exception as e:
        print(f"Ошибка при запуске revenue.py: {e}")
        print("Попробуйте запустить его вручную: python revenue.py")

    print("ВСЯ РАБОТА ЗАВЕРШЕНА!")



if __name__ == "__main__":
    asyncio.run(main())