import requests
import os
from datetime import datetime
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import re

def create_session():
    """Создает сессию с настройками повторных попыток"""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504, 429],
        allowed_methods=["GET", "POST", "HEAD", "OPTIONS"],
        respect_retry_after_header=True
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def normalize_filename(filename):
    """Нормализует имя файла, убирая дублирование расширения"""
    if filename.endswith('.xml.xml'):
        return filename[:-4]
    return filename

def extract_date_from_filename(filename):
    """Извлекает дату из имени файла"""
    # Ищем паттерн YYYYMMDD в имени файла
    match = re.search(r'(\d{8})', filename)
    if match:
        try:
            date_str = match.group(1)
            return datetime.strptime(date_str, '%Y%m%d')
        except ValueError:
            return datetime.min
    
    # Если не нашли дату в формате YYYYMMDD, пробуем найти год и месяц
    year_match = re.search(r'(\d{4})', filename)
    month_match = re.search(r'-(\d{1,2})[.-]', filename)
    
    if year_match and month_match:
        try:
            year = int(year_match.group(1))
            month = int(month_match.group(1))
            return datetime(year, month, 1)
        except ValueError:
            return datetime.min
    
    return datetime.min

def sort_files_by_date(files):
    """Сортирует файлы по дате в имени в порядке убывания"""
    return sorted(files, key=lambda x: extract_date_from_filename(os.path.basename(x)), reverse=True)

def download_file(url, filename, session, verbose=True):
    """Скачивает файл с отображением прогресса"""
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'Connection': 'keep-alive'
    }

    try:
        # Убираем дублирование расширения в имени файла
        basename = os.path.basename(filename)
        basename = normalize_filename(basename)
        filename = os.path.join(os.path.dirname(filename), basename)
        
        # Скачиваем файл
        if verbose:
            print(f"\nСкачивание файла: {basename}")
            print(f"URL: {url}")
        
        response = session.get(url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()
        
        # Получаем размер файла
        total_size = int(response.headers.get('content-length', 0))
        
        # Создаем прогресс-бар
        progress_bar = tqdm(
            total=total_size,
            unit='iB',
            unit_scale=True,
            desc=f"Скачивание {basename}",
            leave=False
        )

        # Создаем директорию для сохранения, если она не существует
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        # Сохраняем файл с отображением прогресса
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    size = f.write(chunk)
                    progress_bar.update(size)
        
        progress_bar.close()
        if verbose:
            print(f"✓ Файл успешно скачан: {basename}")
        return True

    except requests.exceptions.RequestException as e:
        if verbose:
            print(f"✗ Ошибка при скачивании: {str(e)}")
            
            # Проверяем, является ли ошибка 502
            if "502" in str(e):
                print("Получена ошибка 502 (Bad Gateway). Пропускаем файл для повторной попытки позже.")
        return "skip"

def process_list_xml(list_xml_path, session):
    """Обрабатывает list.xml файл и скачивает XML файлы"""
    print(f"\n{'='*80}")
    print(f"Обработка файла: {list_xml_path}")
    print(f"{'='*80}")
    
    try:
        # Парсим XML файл
        tree = ET.parse(list_xml_path)
        root = tree.getroot()
        
        # Выводим корневой элемент и его атрибуты
        print(f"\nКорневой элемент: {root.tag}")
        print(f"Атрибуты корневого элемента: {root.attrib}")
        
        # Создаем директории для сохранения файлов
        xml_dir = os.path.dirname(list_xml_path)
        data_dir = os.path.join(xml_dir, "data")
        os.makedirs(data_dir, exist_ok=True)
        
        # Создаем файл для отслеживания статуса обработки файлов
        status_file = os.path.join(data_dir, "processing_status.json")
        processing_status = {}
        if os.path.exists(status_file):
            try:
                with open(status_file, 'r', encoding='utf-8') as f:
                    processing_status = json.load(f)
                print(f"\nЗагружен статус обработки для {len(processing_status)} файлов")
            except json.JSONDecodeError:
                print("Ошибка при чтении файла статуса, создаем новый")
                processing_status = {}
        
        # Собираем все ссылки на XML файлы
        xml_links = []
        for item in root.findall(".//item"):
            link = item.get('link')
            if link and link.endswith('.xml'):
                xml_filename = normalize_filename(os.path.basename(link))
                if xml_filename not in processing_status or processing_status[xml_filename].get('status') != 'completed':
                    xml_links.append(link)
        
        # Сортируем XML файлы по дате
        xml_links = sort_files_by_date(xml_links)
        if not xml_links:
            print("\nНе найдено XML файлов для обработки")
            return
        
        print(f"\nНайдено {len(xml_links)} XML файлов для обработки")
        
        # Скачиваем XML файлы
        print("\nСкачивание XML файлов...")
        downloaded_xml_files = []
        with tqdm(total=len(xml_links), desc="XML файлы") as pbar:
            for xml_url in xml_links:
                xml_filename = os.path.join(data_dir, normalize_filename(os.path.basename(xml_url)))
                xml_basename = os.path.basename(xml_filename)
                
                # Скачиваем XML файл
                pbar.set_description(f"XML файлы (скачивание: {xml_basename})")
                result = download_file(xml_url, xml_filename, session)
                if result == "skip":
                    print(f"Пропущен XML файл из-за ошибки 502: {xml_basename}")
                elif not result:
                    print(f"Ошибка при скачивании XML файла: {xml_basename}")
                else:
                    downloaded_xml_files.append(xml_filename)
                    # Обновляем статус обработки
                    if xml_basename not in processing_status:
                        processing_status[xml_basename] = {
                            'status': 'completed',
                            'downloaded_at': datetime.now().isoformat(),
                            'url': xml_url
                        }
                
                pbar.update(1)
                time.sleep(0.5)
        
        # Сохраняем статус обработки
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump(processing_status, f, indent=2, ensure_ascii=False)
        
        print(f"\nОбработка завершена:")
        print(f"- Всего файлов: {len(xml_links)}")
        print(f"- Успешно скачано: {len(downloaded_xml_files)}")
        print(f"- Пропущено: {len(xml_links) - len(downloaded_xml_files)}")
    
    except ET.ParseError as e:
        print(f"\n✗ Ошибка при парсинге list.xml: {str(e)}")
        print("Содержимое файла:")
        with open(list_xml_path, 'r', encoding='utf-8') as f:
            print(f.read())
    except Exception as e:
        print(f"\n✗ Неожиданная ошибка: {str(e)}")
        import traceback
        traceback.print_exc()

def main():
    try:
        # Создаем сессию
        session = create_session()
        
        # Обрабатываем list.xml в директории 248
        list_xml_248 = "xml/248/list.xml"
        if os.path.exists(list_xml_248):
            print(f"\nНайден файл: {list_xml_248}")
            try:
                process_list_xml(list_xml_248, session)
            except Exception as e:
                print(f"\n✗ Ошибка при обработке {list_xml_248}: {str(e)}")
                import traceback
                traceback.print_exc()
        
        # Обрабатываем list.xml в директории no248
        list_xml_no248 = "xml/no248/list.xml"
        if os.path.exists(list_xml_no248):
            print(f"\nНайден файл: {list_xml_no248}")
            try:
                process_list_xml(list_xml_no248, session)
            except Exception as e:
                print(f"\n✗ Ошибка при обработке {list_xml_no248}: {str(e)}")
                import traceback
                traceback.print_exc()
    
    except Exception as e:
        print(f"\n✗ Неожиданная ошибка в main(): {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 