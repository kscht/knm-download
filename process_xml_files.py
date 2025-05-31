import os
import xml.etree.ElementTree as ET
from datetime import datetime
import re
from tqdm import tqdm
import json
from download_xml_files import create_session, download_file, normalize_filename
import zipfile
import requests
import time
from threading import Lock

# Глобальный словарь для отслеживания скачиваемых файлов
downloading_files = {}
downloading_lock = Lock()

def get_current_year_month():
    """Возвращает текущий год и месяц"""
    now = datetime.now()
    return now.year, now.month

def find_latest_xml_files(base_dir):
    """Находит все XML файлы в директориях 248/data и no248/data и сортирует их по дате"""
    print("\nИщем все XML файлы")
    xml_files = []  # Список для хранения путей к файлам и их дат
    
    # Проверяем обе директории
    for subdir in ['248', 'no248']:
        dir_path = os.path.join(base_dir, 'xml', subdir, 'data')
        print(f"\nПроверяем директорию: {dir_path}")
        
        if not os.path.exists(dir_path):
            print(f"Директория не найдена: {dir_path}")
            continue
        
        # Выводим все XML файлы в директории
        files = [f for f in os.listdir(dir_path) if f.endswith('.xml')]
        print(f"Найдено XML файлов в {subdir}/data: {len(files)}")
        
        # Добавляем файлы с их датами
        for file in files:
            full_path = os.path.join(dir_path, file)
            date = extract_date_from_xml_filename(file)
            xml_files.append((full_path, date))
            print(f"  - {file} (дата: {date})")
    
    if not xml_files:
        print("\nНе найдено XML файлов")
        return []
    
    # Сортируем файлы по дате в порядке убывания
    sorted_files = sorted(xml_files, key=lambda x: x[1] if x[1] else '000000', reverse=True)
    
    print(f"\nНайдено {len(sorted_files)} файлов, отсортированных по дате:")
    for file, date in sorted_files:
        print(f"  - {os.path.basename(file)} (дата: {date})")
    
    return [file for file, _ in sorted_files]

def get_target_directory(filename, source_dir):
    """Определяет целевую директорию на основе имени файла и исходной директории"""
    # Определяем поддиректорию (248 или no248)
    if 'xml/248' in source_dir:
        subdir = '248'
    elif 'xml/no248' in source_dir:
        subdir = 'no248'
    else:
        print(f"Неизвестная исходная директория: {source_dir}")
        return None
    
    print(f"\nОпределение директории для файла: {filename}")
    print(f"Исходная директория: {source_dir}")
    print(f"Поддиректория: {subdir}")
    
    # Ищем год и месяц в имени файла (формат: 7710146102-inspection-2021-7.xml)
    year_match = re.search(r'inspection-(\d{4})-(\d{1,2})', filename)
    if year_match:
        year = year_match.group(1)
        month = year_match.group(2).zfill(2)  # Добавляем ведущий ноль для месяцев < 10
        target_dir = os.path.join(subdir, f"{year}-{month}")
        print(f"Найден год и месяц: {year}-{month}")
        print(f"Целевая директория: {target_dir}")
        return target_dir
    
    print(f"Не удалось определить целевую директорию для файла: {filename}")
    return None

def extract_links_from_xml(xml_root):
    """Извлекает все ссылки на ZIP и XSD файлы из XML"""
    # Словари для хранения уникальных ссылок с их метаданными
    zip_links = {}  # {filename: {'url': url, 'date': date}}
    xsd_links = {}  # {filename: {'url': url, 'date': date}}
    
    # Список атрибутов для поиска ссылок
    link_attributes = ['link', 'href', 'url', 'file', 'source']
    
    def extract_date_from_element(elem):
        """Извлекает дату из элемента XML"""
        # Ищем дату в атрибутах
        date_attrs = ['date', 'datetime', 'time', 'created', 'modified']
        for attr in date_attrs:
            if elem.get(attr):
                try:
                    return datetime.fromisoformat(elem.get(attr).replace('Z', '+00:00'))
                except ValueError:
                    continue
        
        # Ищем дату в дочерних элементах
        date_elements = ['date', 'datetime', 'time', 'created', 'modified']
        for date_elem in date_elements:
            child = elem.find(date_elem)
            if child is not None and child.text:
                try:
                    return datetime.fromisoformat(child.text.replace('Z', '+00:00'))
                except ValueError:
                    continue
        
        return datetime.min
    
    # Поиск по атрибутам
    for attr in link_attributes:
        for elem in xml_root.findall(f".//*[@{attr}]"):
            link = elem.get(attr)
            if link:
                filename = os.path.basename(link)
                date = extract_date_from_element(elem)
                
                if link.endswith('.zip'):
                    if filename not in zip_links or date > zip_links[filename]['date']:
                        zip_links[filename] = {'url': link, 'date': date}
                elif link.endswith('.xsd'):
                    if filename not in xsd_links or date > xsd_links[filename]['date']:
                        xsd_links[filename] = {'url': link, 'date': date}
    
    # Поиск по тексту элементов
    for elem in xml_root.findall(".//*"):
        if elem.text:
            text = elem.text.strip()
            if text.endswith('.zip') or text.endswith('.xsd'):
                filename = os.path.basename(text)
                date = extract_date_from_element(elem)
                
                if text.endswith('.zip'):
                    if filename not in zip_links or date > zip_links[filename]['date']:
                        zip_links[filename] = {'url': text, 'date': date}
                elif text.endswith('.xsd'):
                    if filename not in xsd_links or date > xsd_links[filename]['date']:
                        xsd_links[filename] = {'url': text, 'date': date}
    
    # Сортируем файлы по дате и возвращаем только URL'ы
    sorted_zip_links = [data['url'] for filename, data in 
                       sorted(zip_links.items(), key=lambda x: x[1]['date'])]
    sorted_xsd_links = [data['url'] for filename, data in 
                       sorted(xsd_links.items(), key=lambda x: x[1]['date'])]
    
    return sorted_zip_links, sorted_xsd_links

def check_file_integrity(file_path):
    """Проверяет целостность файла"""
    if not os.path.exists(file_path):
        return False
        
    # Для ZIP файлов проверяем целостность архива
    if file_path.endswith('.zip'):
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                # Проверяем целостность всех файлов в архиве
                return zip_ref.testzip() is None
        except zipfile.BadZipFile:
            return False
        except Exception as e:
            print(f"Ошибка при проверке ZIP файла {file_path}: {str(e)}")
            return False
    
    # Для остальных файлов проверяем, что файл не пустой
    try:
        return os.path.getsize(file_path) > 0
    except Exception as e:
        print(f"Ошибка при проверке файла {file_path}: {str(e)}")
        return False

def get_file_size(url, session):
    """Получает размер файла по URL"""
    try:
        response = session.head(url, allow_redirects=True)
        if response.status_code == 200:
            return int(response.headers.get('content-length', 0))
    except Exception as e:
        print(f"Ошибка при получении размера файла {url}: {str(e)}")
    return 0

def is_file_downloading(url):
    """Проверяет, скачивается ли файл в данный момент"""
    with downloading_lock:
        return url in downloading_files

def mark_file_downloading(url, is_downloading=True):
    """Отмечает файл как скачиваемый или освобождает его"""
    with downloading_lock:
        if is_downloading:
            downloading_files[url] = True
        else:
            downloading_files.pop(url, None)

def download_with_rate_limit(url, target_path, session, chunk_size=8192, rate_limit=10*1024*1024):
    """Скачивает файл с ограничением скорости"""
    try:
        # Проверяем, не скачивается ли уже этот файл
        if is_file_downloading(url):
            print(f"Файл {os.path.basename(url)} уже скачивается в другом потоке")
            return False

        # Отмечаем файл как скачиваемый
        mark_file_downloading(url, True)

        # Добавляем базовые заголовки браузера
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }

        response = session.get(url, stream=True, headers=headers)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        start_time = time.time()
        
        with open(target_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Ограничение скорости
                    elapsed = time.time() - start_time
                    expected_time = downloaded / rate_limit
                    if elapsed < expected_time:
                        time.sleep(expected_time - elapsed)
        
        return True
    except Exception as e:
        print(f"Ошибка при скачивании {url}: {str(e)}")
        if os.path.exists(target_path):
            os.remove(target_path)
        return False
    finally:
        # Освобождаем файл
        mark_file_downloading(url, False)

def download_and_check_file(file_info):
    """Функция для скачивания и проверки одного файла"""
    url, target_path, session, force_update = file_info
    basename = os.path.basename(url)
    
    # Проверяем существование и целостность файла, если не требуется принудительное обновление
    if not force_update and os.path.exists(target_path) and check_file_integrity(target_path):
        return f"Пропущен файл (уже скачан и цел): {basename}", True
    
    # Скачиваем файл с ограничением скорости
    result = download_with_rate_limit(url, target_path, session)
    if result:
        return f"✓ Файл успешно скачан: {basename}", True
    else:
        return f"✗ Ошибка при скачивании файла: {basename}", False

def extract_date_from_filename(filename):
    """Извлекает дату из имени файла в формате YYYYMMDD"""
    # Ищем дату в формате YYYYMMDD
    match = re.search(r'data-(\d{8})', filename)
    if match:
        return match.group(1)
    return None

def extract_date_from_xml_filename(filename):
    """Извлекает дату из имени XML файла в формате YYYY-MM"""
    # Ищем дату в формате inspection-YYYY-M
    match = re.search(r'inspection-(\d{4})-(\d{1,2})', filename)
    if match:
        year = match.group(1)
        month = match.group(2).zfill(2)  # Добавляем ведущий ноль для месяцев < 10
        return f"{year}{month}"
    return None

def process_xml_files(base_dir=".", force_update=False):
    """Обрабатывает XML файлы и скачивает связанные файлы
    
    Args:
        base_dir (str): Базовая директория для скачивания файлов
        force_update (bool): Если True, то файлы будут перескачаны даже если они уже существуют
    """
    # Создаем сессию с увеличенным таймаутом
    session = create_session()
    session.timeout = 300  # 5 минут таймаут
    
    # Создаем базовые директории для данных
    data_base_dir = os.path.join(base_dir, "data")
    xsd_base_dir = os.path.join(base_dir, "xsd")
    
    print("\nНачинаем новую сессию скачивания")
    if force_update:
        print("Режим принудительного обновления: все файлы будут перескачаны")
    
    # Находим XML файлы (уже отсортированные по дате)
    latest_files = find_latest_xml_files(base_dir)
    if not latest_files:
        print("Не найдены XML файлы")
        return
    
    print(f"\nНайдено {len(latest_files)} XML файлов для обработки")
    
    # Счетчик общего количества файлов
    total_files_processed = 0
    
    # Обрабатываем каждый XML файл последовательно
    for xml_file in latest_files:
        try:
            print(f"\nОбработка XML файла: {xml_file}")
            tree = ET.parse(xml_file)
            root = tree.getroot()
            zip_links, xsd_links = extract_links_from_xml(root)
            
            # Определяем целевую директорию
            xml_basename = os.path.basename(xml_file)
            target_dir = get_target_directory(xml_basename, xml_file)
            if not target_dir:
                print(f"\nПропущен файл {xml_basename}: не удалось определить целевую директорию")
                continue
            
            # Создаем полный путь к целевой директории
            full_target_dir = os.path.join(data_base_dir, target_dir)
            os.makedirs(full_target_dir, exist_ok=True)
            os.makedirs(xsd_base_dir, exist_ok=True)
            
            # Собираем все файлы из текущего XML
            files_to_download = []
            
            # Добавляем ZIP файлы
            for zip_link in zip_links:
                zip_basename = os.path.basename(zip_link)
                zip_filename = os.path.join(full_target_dir, zip_basename)
                date = extract_date_from_filename(zip_basename)
                files_to_download.append((zip_link, zip_filename, date))
            
            # Добавляем XSD файлы
            for xsd_link in xsd_links:
                xsd_basename = os.path.basename(xsd_link)
                xsd_filename = os.path.join(xsd_base_dir, xsd_basename)
                date = extract_date_from_filename(xsd_basename)
                files_to_download.append((xsd_link, xsd_filename, date))
            
            # Сортируем файлы по дате в порядке убывания
            files_to_download.sort(key=lambda x: x[2] if x[2] else '00000000', reverse=True)
            
            total_files = len(files_to_download)
            print(f"\nВ файле {xml_basename} найдено {len(zip_links)} ZIP и {len(xsd_links)} XSD файлов")
            print(f"Начинаем скачивание {total_files} файлов")
            
            # Скачиваем файлы из текущего XML
            for i, (url, target_path, date) in enumerate(files_to_download, 1):
                try:
                    print(f"\nСкачивание файла {i} из {total_files} в {xml_basename}: {os.path.basename(url)}")
                    print(f"URL: {url}")
                    print(f"Сохраняем в: {target_path}")
                    print(f"Дата файла: {date}")
                    print(f"Источник: {xml_basename}")
                    message, success = download_and_check_file((url, target_path, session, force_update))
                    print(f"{message}")
                    total_files_processed += 1
                except Exception as e:
                    print(f"Ошибка при скачивании {url}: {str(e)}")
                    continue
            
            print(f"\nЗавершена обработка файла {xml_basename}")
            print(f"Всего обработано файлов: {total_files_processed}")
            
        except Exception as e:
            print(f"Ошибка при обработке XML файла {xml_file}: {str(e)}")
            continue
    
    print(f"\nЗавершена обработка всех XML файлов")
    print(f"Всего скачано файлов: {total_files_processed}")

if __name__ == "__main__":
    process_xml_files(force_update=True)  # По умолчанию включаем принудительное обновление 