import requests
import os
from datetime import datetime
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import signal

def create_session():
    """Создает сессию с настройками повторных попыток"""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,  # увеличиваем количество попыток
        backoff_factor=2,  # увеличиваем время ожидания между попытками
        status_forcelist=[500, 502, 503, 504, 429],  # добавляем 429 (Too Many Requests)
        allowed_methods=["GET", "POST", "HEAD", "OPTIONS"],
        respect_retry_after_header=True  # учитываем заголовок Retry-After от сервера
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def check_zip_integrity(filename, verbose=True, timeout=30):
    """Проверяет целостность zip-архива"""
    if not filename.endswith('.zip'):
        return True  # Для не-ZIP файлов считаем, что они целы
        
    try:
        # Добавляем таймаут для открытия файла
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            # Проверяем целостность архива
            if verbose:
                print(f"\nПроверка архива: {os.path.basename(filename)}")
                print(f"Размер файла: {os.path.getsize(filename)} байт")
                
                # Получаем список файлов в архиве
                file_list = zip_ref.namelist()
                print(f"Файлов в архиве: {len(file_list)}")
                if file_list:
                    print("Содержимое архива:")
                    for file in file_list:
                        info = zip_ref.getinfo(file)
                        print(f"- {file} ({info.file_size} байт)")
            
            # Проверяем целостность с таймаутом
            def timeout_handler(signum, frame):
                raise TimeoutError(f"Проверка файла {filename} превысила таймаут {timeout} секунд")
            
            # Устанавливаем обработчик таймаута
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)
            
            try:
                test_result = zip_ref.testzip()
                # Отключаем таймаут
                signal.alarm(0)
                
                if test_result is None:
                    if verbose:
                        print("✓ Архив цел")
                    return True
                else:
                    if verbose:
                        print(f"✗ Архив поврежден. Первый поврежденный файл: {test_result}")
                    return False
            except TimeoutError as e:
                if verbose:
                    print(f"✗ {str(e)}")
                return False
            finally:
                # Гарантируем отключение таймаута
                signal.alarm(0)
                
    except zipfile.BadZipFile as e:
        if verbose:
            print(f"✗ Ошибка при открытии архива: {str(e)}")
        return False
    except Exception as e:
        if verbose:
            print(f"✗ Неожиданная ошибка при проверке архива: {str(e)}")
        return False

def check_files_integrity(files_to_check, integrity_cache_file):
    """Проверяет целостность списка файлов в многопоточном режиме"""
    # Загружаем кэш результатов проверки
    integrity_cache = {}
    if os.path.exists(integrity_cache_file):
        try:
            with open(integrity_cache_file, 'r', encoding='utf-8') as f:
                integrity_cache = json.load(f)
            print(f"Загружен кэш проверки целостности: {len(integrity_cache)} файлов")
        except json.JSONDecodeError:
            print("Ошибка при чтении кэша проверки целостности, создаем новый")
            integrity_cache = {}
    
    # Фильтруем файлы, которые уже проверены и не изменились
    files_to_check_now = []
    results = {}
    skipped_files = 0
    
    print("\nАнализ файлов для проверки...")
    for file in tqdm(files_to_check, desc="Анализ файлов"):
        try:
            file_stat = os.stat(file)
            file_info = {
                'size': file_stat.st_size,
                'mtime': file_stat.st_mtime
            }
            
            if file in integrity_cache:
                cached_info = integrity_cache[file]
                if (cached_info['size'] == file_info['size'] and 
                    cached_info['mtime'] == file_info['mtime']):
                    results[file] = cached_info['is_valid']
                    skipped_files += 1
                    continue
            
            files_to_check_now.append(file)
        except Exception as e:
            print(f"\nОшибка при анализе файла {file}: {str(e)}")
            continue
    
    if not files_to_check_now:
        print(f"\nВсе файлы ({len(files_to_check)}) уже проверены и не изменились")
        return results
    
    print(f"\nСтатистика проверки:")
    print(f"- Всего файлов: {len(files_to_check)}")
    print(f"- Пропущено (из кэша): {skipped_files}")
    print(f"- Требует проверки: {len(files_to_check_now)}")
    
    # Разбиваем файлы на батчи для более эффективной обработки
    batch_size = 1000
    total_batches = (len(files_to_check_now) + batch_size - 1) // batch_size
    
    print(f"\nНачинаем проверку файлов (батчами по {batch_size})...")
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(files_to_check_now))
        current_batch = files_to_check_now[start_idx:end_idx]
        
        print(f"\nОбработка батча {batch_num + 1}/{total_batches} ({len(current_batch)} файлов)")
        
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Создаем словарь будущих результатов
            future_to_file = {executor.submit(check_zip_integrity, file, verbose=False): file for file in current_batch}
            
            # Создаем прогресс-бар
            with tqdm(total=len(current_batch), desc=f"Батч {batch_num + 1}/{total_batches}") as pbar:
                # Обрабатываем результаты по мере их завершения
                for future in as_completed(future_to_file):
                    file = future_to_file[future]
                    try:
                        # Добавляем таймаут для получения результата
                        result = future.result(timeout=60)  # 60 секунд на получение результата
                        results[file] = result
                        
                        # Сохраняем информацию о файле и результате проверки
                        file_stat = os.stat(file)
                        integrity_cache[file] = {
                            'size': file_stat.st_size,
                            'mtime': file_stat.st_mtime,
                            'is_valid': result
                        }
                    except TimeoutError:
                        print(f"\nТаймаут при проверке файла {file}")
                        results[file] = False
                    except Exception as e:
                        print(f"\nОшибка при проверке файла {file}: {str(e)}")
                        results[file] = False
                    pbar.update(1)
        
        # Сохраняем кэш после каждого батча
        try:
            with open(integrity_cache_file, 'w', encoding='utf-8') as f:
                json.dump(integrity_cache, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"\nОшибка при сохранении кэша: {str(e)}")
    
    # Подсчитываем статистику результатов
    valid_files = sum(1 for result in results.values() if result)
    invalid_files = sum(1 for result in results.values() if not result)
    
    print(f"\nИтоговая статистика проверки:")
    print(f"- Всего проверено: {len(results)}")
    print(f"- Целых файлов: {valid_files}")
    print(f"- Поврежденных файлов: {invalid_files}")
    
    return results

def download_file(url, filename, session):
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
        # Убираем все дублирующиеся расширения .zip
        while basename.endswith('.zip.zip'):
            basename = basename[:-4]
        # Убираем дублирующиеся расширения .xml
        while basename.endswith('.xml.xml'):
            basename = basename[:-4]
        filename = os.path.join(os.path.dirname(filename), basename)
        
        # Скачиваем файл
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
        print(f"✓ Файл успешно скачан: {basename}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"✗ Ошибка при скачивании: {str(e)}")
        
        # Проверяем, является ли ошибка 502
        if "502" in str(e):
            print("Получена ошибка 502 (Bad Gateway). Пропускаем файл для повторной попытки позже.")
            return "skip"  # Возвращаем специальный статус для пропуска
        
        return "skip"

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

def get_unique_latest_files(files):
    """Возвращает только последние версии файлов, убирая дубликаты"""
    # Создаем словарь для хранения последних версий файлов
    latest_files = {}
    
    for file in files:
        # Извлекаем базовое имя файла без даты
        base_name = re.sub(r'\d{8}|\d{4}-\d{1,2}', '', os.path.basename(file))
        base_name = re.sub(r'[.-]', '', base_name)
        
        # Получаем дату из имени файла
        file_date = extract_date_from_filename(file)
        
        # Если это первый файл с таким базовым именем или дата новее
        if base_name not in latest_files or file_date > extract_date_from_filename(latest_files[base_name]):
            latest_files[base_name] = file
    
    return list(latest_files.values())

def sort_files_by_date(files):
    """Сортирует файлы по дате в имени в порядке убывания и убирает дубликаты"""
    # Сначала получаем только последние версии файлов
    unique_files = get_unique_latest_files(files)
    
    # Сортируем по дате
    return sorted(unique_files, key=lambda x: extract_date_from_filename(os.path.basename(x)), reverse=True)

def extract_year_month_from_url(url):
    """Извлекает год и месяц из URL"""
    # Проверяем оба формата URL: /erknm-plan/YYYY/MM/ и /erknm-opendata/YYYY/MM/
    match = re.search(r'/erknm-(?:plan|opendata)/(\d{4})/(\d{1,2})/', url)
    if match:
        year = match.group(1)
        month = match.group(2).zfill(2)  # Добавляем ведущий ноль для месяцев < 10
        return year, month
    return None, None

def extract_links_from_xml(xml_root):
    """Извлекает все ссылки на ZIP и XSD файлы из XML"""
    zip_links = set()
    xsd_links = set()
    
    # Список атрибутов для поиска ссылок
    link_attributes = ['link', 'href', 'url', 'file', 'source']
    
    # Поиск по атрибутам
    for attr in link_attributes:
        for elem in xml_root.findall(f".//*[@{attr}]"):
            link = elem.get(attr)
            if link:
                if link.endswith('.zip'):
                    zip_links.add(link)
                elif link.endswith('.xsd'):
                    xsd_links.add(link)
    
    # Поиск по тексту элементов
    for elem in xml_root.findall(".//*"):
        if elem.text:
            text = elem.text.strip()
            if text.endswith('.zip'):
                zip_links.add(text)
            elif text.endswith('.xsd'):
                xsd_links.add(text)
    
    return zip_links, xsd_links

def normalize_filename(filename):
    """Нормализует имя файла, убирая дублирование расширения"""
    if filename.endswith('.xml.xml'):
        return filename[:-4]
    return filename

def process_list_xml(list_xml_path, session, pbar=None):
    """Обрабатывает list.xml файл и скачивает связанные файлы"""
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
        xsd_dir = os.path.join(xml_dir, "xsd")
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(xsd_dir, exist_ok=True)
        
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
        
        # Создаем файл для кэша проверки целостности
        integrity_cache_file = os.path.join(data_dir, "integrity_cache.json")
        
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
        
        # Скачиваем все XML файлы
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
                
                pbar.update(1)
                time.sleep(0.5)
        
        if not downloaded_xml_files:
            print("\nНе удалось скачать ни одного XML файла")
            return
        
        # Собираем все ссылки на ZIP и XSD файлы из всех XML
        all_zip_links = set()
        all_xsd_links = set()
        latest_zip_urls = {}  # Словарь для хранения самых свежих URL для каждого файла
        latest_xsd_urls = {}  # Словарь для хранения самых свежих URL для каждого файла
        
        print("\nАнализ XML файлов для поиска актуальных ссылок...")
        for xml_file in downloaded_xml_files:
            try:
                xml_tree = ET.parse(xml_file)
                xml_root = xml_tree.getroot()
                zip_links, xsd_links = extract_links_from_xml(xml_root)
                
                # Получаем базовый путь для URL
                xml_dir_path = os.path.dirname(xml_file)
                xml_url_base = os.path.dirname(xml_links[0]) if xml_links else ""
                
                # Обрабатываем ZIP ссылки
                for zip_link in zip_links:
                    zip_basename = os.path.basename(zip_link)
                    if zip_basename not in latest_zip_urls:
                        latest_zip_urls[zip_basename] = os.path.join(xml_url_base, zip_basename)
                    all_zip_links.add(zip_basename)
                
                # Обрабатываем XSD ссылки
                for xsd_link in xsd_links:
                    xsd_basename = os.path.basename(xsd_link)
                    if xsd_basename not in latest_xsd_urls:
                        latest_xsd_urls[xsd_basename] = os.path.join(xml_url_base, xsd_basename)
                    all_xsd_links.add(xsd_basename)
                
            except ET.ParseError as e:
                print(f"\n✗ Ошибка при парсинге XML файла {os.path.basename(xml_file)}: {str(e)}")
                continue
        
        print(f"\nНайдено {len(all_zip_links)} уникальных ZIP файлов")
        print(f"Найдено {len(all_xsd_links)} уникальных XSD файлов")
        
        # Проверяем существующие файлы
        existing_zip_files = [os.path.join(data_dir, basename) for basename in all_zip_links if os.path.exists(os.path.join(data_dir, basename))]
        existing_xsd_files = [os.path.join(xsd_dir, basename) for basename in all_xsd_links if os.path.exists(os.path.join(xsd_dir, basename))]
        
        # Создаем словари для хранения результатов проверки целостности
        zip_integrity_results = {}
        xsd_integrity_results = {}
        
        if existing_zip_files:
            print("\nПроверка целостности существующих ZIP файлов...")
            zip_integrity_results = check_files_integrity(existing_zip_files, integrity_cache_file)
        
        if existing_xsd_files:
            print("\nПроверка целостности существующих XSD файлов...")
            xsd_integrity_results = check_files_integrity(existing_xsd_files, integrity_cache_file)
        
        # Скачиваем ZIP файлы
        print("\nОбработка ZIP файлов...")
        with tqdm(total=len(all_zip_links), desc="ZIP файлы") as pbar:
            for zip_basename in all_zip_links:
                zip_url = latest_zip_urls[zip_basename]
                zip_filename = os.path.join(data_dir, zip_basename)
                
                # Проверяем статус файла
                if os.path.exists(zip_filename):
                    if zip_filename in zip_integrity_results and zip_integrity_results[zip_filename]:
                        pbar.set_description(f"ZIP файлы (существующий: {zip_basename})")
                        pbar.update(1)
                        continue
                    else:
                        print(f"\nФайл поврежден, будет перескачан: {zip_basename}")
                        os.remove(zip_filename)
                
                # Скачиваем ZIP файл
                pbar.set_description(f"ZIP файлы (скачивание: {zip_basename})")
                result = download_file(zip_url, zip_filename, session)
                if result == "skip":
                    print(f"Пропущен ZIP файл из-за ошибки 502: {zip_basename}")
                elif not result:
                    print(f"Ошибка при скачивании ZIP файла: {zip_basename}")
                else:
                    # Проверяем целостность только что скачанного файла
                    if not check_zip_integrity(zip_filename, verbose=True):
                        print(f"✗ Скачанный файл поврежден: {zip_basename}")
                        os.remove(zip_filename)
                
                pbar.update(1)
                time.sleep(0.5)
        
        # Скачиваем XSD файлы
        print("\nОбработка XSD файлов...")
        with tqdm(total=len(all_xsd_links), desc="XSD файлы") as pbar:
            for xsd_basename in all_xsd_links:
                xsd_url = latest_xsd_urls[xsd_basename]
                xsd_filename = os.path.join(xsd_dir, xsd_basename)
                
                # Проверяем статус файла
                if os.path.exists(xsd_filename):
                    if xsd_filename in xsd_integrity_results and xsd_integrity_results[xsd_filename]:
                        pbar.set_description(f"XSD файлы (существующий: {xsd_basename})")
                        pbar.update(1)
                        continue
                    else:
                        print(f"\nФайл поврежден, будет перескачан: {xsd_basename}")
                        os.remove(xsd_filename)
                
                # Скачиваем XSD файл
                pbar.set_description(f"XSD файлы (скачивание: {xsd_basename})")
                result = download_file(xsd_url, xsd_filename, session)
                if result == "skip":
                    print(f"Пропущен XSD файл из-за ошибки 502: {xsd_basename}")
                elif not result:
                    print(f"Ошибка при скачивании XSD файла: {xsd_basename}")
                
                pbar.update(1)
                time.sleep(0.5)
        
        # Обновляем статус обработки
        for xml_file in downloaded_xml_files:
            xml_basename = os.path.basename(xml_file)
            if xml_basename not in processing_status:
                processing_status[xml_basename] = {
                    'status': 'in_progress',
                    'xml_downloaded': True,
                    'zip_files': [],
                    'xsd_files': [],
                    'errors': []
                }
            
            # Проверяем наличие ZIP файлов
            for zip_basename in all_zip_links:
                zip_filename = os.path.join(data_dir, zip_basename)
                
                if os.path.exists(zip_filename):
                    if zip_filename in zip_integrity_results and zip_integrity_results[zip_filename]:
                        processing_status[xml_basename]['zip_files'].append({
                            'filename': zip_basename,
                            'status': 'exists'
                        })
                    else:
                        processing_status[xml_basename]['zip_files'].append({
                            'filename': zip_basename,
                            'status': 'failed'
                        })
                        processing_status[xml_basename]['errors'].append(f"ZIP файл поврежден: {zip_basename}")
                else:
                    processing_status[xml_basename]['zip_files'].append({
                        'filename': zip_basename,
                        'status': 'missing'
                    })
                    processing_status[xml_basename]['errors'].append(f"ZIP файл отсутствует: {zip_basename}")
            
            # Проверяем наличие XSD файлов
            for xsd_basename in all_xsd_links:
                xsd_filename = os.path.join(xsd_dir, xsd_basename)
                
                if os.path.exists(xsd_filename):
                    if xsd_filename in xsd_integrity_results and xsd_integrity_results[xsd_filename]:
                        processing_status[xml_basename]['xsd_files'].append({
                            'filename': xsd_basename,
                            'status': 'exists'
                        })
                    else:
                        processing_status[xml_basename]['xsd_files'].append({
                            'filename': xsd_basename,
                            'status': 'failed'
                        })
                        processing_status[xml_basename]['errors'].append(f"XSD файл поврежден: {xsd_basename}")
                else:
                    processing_status[xml_basename]['xsd_files'].append({
                        'filename': xsd_basename,
                        'status': 'missing'
                    })
                    processing_status[xml_basename]['errors'].append(f"XSD файл отсутствует: {xsd_basename}")
            
            # Проверяем, все ли файлы успешно обработаны
            all_zip_success = all(f['status'] == 'exists' for f in processing_status[xml_basename]['zip_files'])
            all_xsd_success = all(f['status'] == 'exists' for f in processing_status[xml_basename]['xsd_files'])
            
            if all_zip_success and all_xsd_success and not processing_status[xml_basename]['errors']:
                processing_status[xml_basename]['status'] = 'completed'
            else:
                processing_status[xml_basename]['status'] = 'incomplete'
            
            print(f"\nСтатус обработки файла {xml_basename}:")
            print(f"- ZIP файлов: {len(processing_status[xml_basename]['zip_files'])}")
            print(f"- XSD файлов: {len(processing_status[xml_basename]['xsd_files'])}")
            print(f"- Ошибок: {len(processing_status[xml_basename]['errors'])}")
            print(f"- Статус: {processing_status[xml_basename]['status']}")
        
        # Сохраняем статус обработки
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump(processing_status, f, indent=2, ensure_ascii=False)
    
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
        
        # Подсчитываем общее количество файлов для скачивания
        total_files = 0
        list_xml_248 = "xml/248/list.xml"
        list_xml_no248 = "xml/no248/list.xml"
        
        if os.path.exists(list_xml_248):
            try:
                tree = ET.parse(list_xml_248)
                root = tree.getroot()
                total_files += len(root.findall(".//item"))
            except ET.ParseError as e:
                print(f"\n✗ Ошибка при парсинге {list_xml_248}: {str(e)}")
        
        if os.path.exists(list_xml_no248):
            try:
                tree = ET.parse(list_xml_no248)
                root = tree.getroot()
                total_files += len(root.findall(".//item"))
            except ET.ParseError as e:
                print(f"\n✗ Ошибка при парсинге {list_xml_no248}: {str(e)}")
        
        if total_files == 0:
            print("Не найдено файлов для обработки")
            return
        
        print(f"Всего файлов для обработки: {total_files}")
        
        # Создаем общий прогресс-бар
        with tqdm(total=total_files, desc="Общий прогресс", position=0) as pbar:
            # Обрабатываем list.xml в директории 248
            if os.path.exists(list_xml_248):
                print(f"\nНайден файл: {list_xml_248}")
                try:
                    process_list_xml(list_xml_248, session, pbar)
                except Exception as e:
                    print(f"\n✗ Ошибка при обработке {list_xml_248}: {str(e)}")
                    import traceback
                    traceback.print_exc()
            
            # Обрабатываем list.xml в директории no248
            if os.path.exists(list_xml_no248):
                print(f"\nНайден файл: {list_xml_no248}")
                try:
                    process_list_xml(list_xml_no248, session, pbar)
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