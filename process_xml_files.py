import os
import xml.etree.ElementTree as ET
from datetime import datetime
import re
from tqdm import tqdm
import json
from download_xml_files import create_session, download_file, normalize_filename
import zipfile
import requests

def get_current_year_month():
    """Возвращает текущий год и месяц"""
    now = datetime.now()
    return now.year, now.month

def find_latest_xml_files(base_dir):
    """Находит все XML файлы в директориях 248/data и no248/data"""
    print("\nИщем все XML файлы")
    latest_files = []  # Список для хранения путей к файлам
    
    # Проверяем обе директории
    for subdir in ['248', 'no248']:
        dir_path = os.path.join(base_dir, 'xml', subdir, 'data')
        print(f"\nПроверяем директорию: {dir_path}")
        
        if not os.path.exists(dir_path):
            print(f"Директория не найдена: {dir_path}")
            continue
        
        # Выводим все XML файлы в директории
        xml_files = [f for f in os.listdir(dir_path) if f.endswith('.xml')]
        print(f"Найдено XML файлов в {subdir}/data: {len(xml_files)}")
        for file in xml_files:
            print(f"  - {file}")
            full_path = os.path.join(dir_path, file)
            print(f"Полный путь: {full_path}")
            latest_files.append(full_path)
    
    if not latest_files:
        print("\nНе найдено XML файлов")
    else:
        print(f"\nНайдено {len(latest_files)} файлов:")
        for file in latest_files:
            print(f"  - {file}")
    
    return latest_files

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
    
    # Ищем год и месяц в имени файла (формат: data-20230902-structure-20220125.zip)
    year_match = re.search(r'data-(\d{4})(\d{2})', filename)
    if year_match:
        year = year_match.group(1)
        month = year_match.group(2)
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

def process_xml_files(base_dir="."):
    """Обрабатывает XML файлы и скачивает связанные файлы"""
    session = create_session()
    
    # Создаем базовые директории для данных
    data_base_dir = os.path.join(base_dir, "data")
    xsd_base_dir = os.path.join(base_dir, "xsd")
    
    print("\nНачинаем новую сессию скачивания")
    
    # Находим XML файлы
    latest_files = find_latest_xml_files(base_dir)
    if not latest_files:
        print("Не найдены XML файлы")
        return
    
    print(f"\nНайдено {len(latest_files)} XML файлов для обработки")
    
    # Удаляем множество processed_files, чтобы обработать все файлы заново
    # processed_files = set()  # Закомментируйте эту строку
    
    # Подсчитываем общее количество файлов для скачивания
    total_files_to_download = 0
    for xml_file in latest_files:
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            zip_links, xsd_links = extract_links_from_xml(root)
            total_files_to_download += len(zip_links) + len(xsd_links)
            print(f"В файле {xml_file} найдено {len(zip_links)} ZIP и {len(xsd_links)} XSD файлов")
        except Exception as e:
            print(f"Ошибка при подсчете файлов в {xml_file}: {str(e)}")
            continue
    
    print(f"\nВсего файлов для скачивания: {total_files_to_download}")
    
    # Создаем общий прогресс-бар
    with tqdm(total=total_files_to_download, desc="Общий прогресс", position=0) as pbar:
        # Обрабатываем каждый XML файл
        for xml_file in tqdm(latest_files, desc="Обработка XML файлов", position=1, leave=False):
            try:
                xml_basename = os.path.basename(xml_file)
                
                # Пропускаем уже обработанные файлы в текущей сессии
                # if xml_basename in processed_files:
                #     print(f"\nПропущен уже обработанный файл: {xml_basename}")
                #     continue
                    
                print(f"\nОбработка файла: {xml_basename}")
                print(f"Полный путь: {xml_file}")
                
                # Парсим XML файл
                tree = ET.parse(xml_file)
                root = tree.getroot()
                
                # Извлекаем ссылки
                zip_links, xsd_links = extract_links_from_xml(root)
                
                if not zip_links and not xsd_links:
                    print(f"В файле {xml_basename} не найдено ссылок на файлы")
                    # processed_files.add(xml_basename)
                    continue
                    
                print(f"Найдено {len(zip_links)} ZIP файлов и {len(xsd_links)} XSD файлов")
                
                # Определяем целевую директорию на основе имени XML файла и исходной директории
                target_dir = get_target_directory(xml_basename, xml_file)
                if not target_dir:
                    print(f"\nПропущен файл {xml_basename}: не удалось определить целевую директорию")
                    # processed_files.add(xml_basename)
                    continue
                
                print(f"Целевая директория: {target_dir}")
                
                # Создаем полный путь к целевой директории
                full_target_dir = os.path.join(data_base_dir, target_dir)
                print(f"Создаем директорию: {full_target_dir}")
                os.makedirs(full_target_dir, exist_ok=True)
                os.makedirs(xsd_base_dir, exist_ok=True)
                
                # Скачиваем ZIP файлы
                for zip_link in zip_links:
                    zip_basename = os.path.basename(zip_link)
                    zip_filename = os.path.join(full_target_dir, zip_basename)
                    
                    # Проверяем существование и целостность файла
                    if os.path.exists(zip_filename) and check_file_integrity(zip_filename):
                        print(f"\nПропущен ZIP файл (уже скачан и цел): {zip_basename}")
                        pbar.update(1)
                        continue
                    
                    # Скачиваем файл
                    print(f"\nСкачивание файла: {zip_basename}")
                    print(f"URL: {zip_link}")
                    result = download_file(zip_link, zip_filename, session, verbose=False)
                    if result == True:
                        print(f"✓ Файл успешно скачан: {zip_basename}")
                    else:
                        print(f"✗ Ошибка при скачивании файла: {zip_basename}")
                    pbar.update(1)
                
                # Скачиваем XSD файлы
                for xsd_link in xsd_links:
                    xsd_basename = os.path.basename(xsd_link)
                    xsd_filename = os.path.join(xsd_base_dir, xsd_basename)
                    
                    # Проверяем существование и целостность файла
                    if os.path.exists(xsd_filename) and check_file_integrity(xsd_filename):
                        print(f"\nПропущен XSD файл (уже скачан и цел): {xsd_basename}")
                        pbar.update(1)
                        continue
                    
                    # Скачиваем файл
                    print(f"\nСкачивание файла: {xsd_basename}")
                    print(f"URL: {xsd_link}")
                    result = download_file(xsd_link, xsd_filename, session, verbose=False)
                    if result == True:
                        print(f"✓ Файл успешно скачан: {xsd_basename}")
                    else:
                        print(f"✗ Ошибка при скачивании файла: {xsd_basename}")
                    pbar.update(1)
                
                # Отмечаем XML файл как обработанный
                # processed_files.add(xml_basename)
                
            except ET.ParseError as e:
                print(f"\n✗ Ошибка при парсинге XML файла {xml_file}: {str(e)}")
                # processed_files.add(xml_basename)
                continue
            except Exception as e:
                print(f"\n✗ Неожиданная ошибка при обработке {xml_file}: {str(e)}")
                import traceback
                traceback.print_exc()
                # processed_files.add(xml_basename)
                continue

if __name__ == "__main__":
    process_xml_files() 