import requests
import os
from datetime import datetime
import time
import re
import json
import zipfile
import argparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

def create_session():
    """Создает сессию с настройками повторных попыток"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,  # количество повторных попыток
        backoff_factor=1,  # время ожидания между попытками
        status_forcelist=[500, 502, 503, 504]  # коды ошибок для повторных попыток
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def check_zip_integrity(filename):
    """Проверяет целостность zip-архива"""
    try:
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            # Проверяем целостность архива
            if zip_ref.testzip() is None:
                # Дополнительно проверяем, что архив не пустой
                if len(zip_ref.namelist()) > 0:
                    return True
            return False
    except zipfile.BadZipFile:
        return False
    except Exception as e:
        print(f"Ошибка при проверке архива: {str(e)}")
        return False

def get_data_url(year, month, is_federal_law_248=False):
    # Формируем URL API
    api_url = f"https://proverki.gov.ru/api/portal/public-open-data/check/{year}/{month}"
    
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Referer': f'https://proverki.gov.ru/portal/public-open-data/check/{year}/{month}?isFederalLaw248={"true" if is_federal_law_248 else "false"}',
        'Origin': 'https://proverki.gov.ru'
    }

    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        
        # Выводим ответ API для анализа
        print(f"\nОтвет API для {year}/{month} (248-ФЗ: {is_federal_law_248}):")
        print(response.text[:1000])  # Выводим первые 1000 символов для анализа
        
        # Пробуем распарсить JSON
        try:
            data = response.json()
            print(f"Данные API: {json.dumps(data, indent=2, ensure_ascii=False)[:1000]}")
            return data
        except json.JSONDecodeError:
            print("Не удалось распарсить JSON")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении данных API для {year}/{month}: {str(e)}")
        return None

def download_file(year, month, is_federal_law_248=False):
    # Определяем папку для сохранения
    folder = 'data/248' if is_federal_law_248 else 'data'
    
    # Формируем имя файла для сохранения
    if is_federal_law_248:
        # Для 248-ФЗ сохраняем информацию о годе и месяце в имени файла
        filename = f"{folder}/data-{year}{month:02d}26-structure-20220125.zip"
    else:
        filename = f"{folder}/data-{year}{month:02d}26-structure-20220125.zip"
    
    # Проверяем существование файла
    if os.path.exists(filename):
        print(f"Файл существует: {filename}")
        # Проверяем целостность существующего файла
        if check_zip_integrity(filename):
            print("Файл цел (проверка zip-архива)")
            return True
        else:
            print("Файл поврежден! Удаляем и скачиваем заново")
            os.remove(filename)
            return download_file(year, month, is_federal_law_248)  # Рекурсивно скачиваем заново

    # Если файл не существует, скачиваем его
    # Используем текущий месяц в URL
    data_url = f"https://proverki.gov.ru/blob/opendata/{year}/{month}/data-20250522-structure-20210222.zip"
    
    # Формируем referer с учетом типа данных
    referer = f"https://proverki.gov.ru/portal/public-open-data/check/{year}/{month}?isFederalLaw248={'true' if is_federal_law_248 else 'false'}"
    
    # Заголовки запроса
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
        'Referer': referer
    }

    # Создаем директорию для сохранения файлов, если она не существует
    if not os.path.exists(folder):
        os.makedirs(folder)

    # Создаем сессию с настройками повторных попыток
    session = create_session()

    try:
        # Скачиваем файл
        print(f"Начинаем скачивание файла: {filename}")
        print(f"URL для скачивания: {data_url}")
        print(f"Referer: {referer}")
        
        response = session.get(data_url, headers=headers, stream=True)
        response.raise_for_status()
        
        # Получаем размер файла
        total_size = int(response.headers.get('content-length', 0))
        
        # Создаем прогресс-бар
        progress_bar = tqdm(
            total=total_size,
            unit='iB',
            unit_scale=True,
            desc=f"Скачивание {os.path.basename(filename)}"
        )

        # Сохраняем файл с отображением прогресса
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    size = f.write(chunk)
                    progress_bar.update(size)
        
        progress_bar.close()
        
        # Проверяем целостность скачанного файла
        if check_zip_integrity(filename):
            print("Файл успешно скачан и проверен (zip-архив цел)")
            return True
        else:
            print("Скачанный файл поврежден! Удаляем и пробуем снова")
            os.remove(filename)
            return download_file(year, month, is_federal_law_248)  # Рекурсивно скачиваем заново

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при скачивании файла для {year}/{month}: {str(e)}")
        return False

def main():
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser(description='Скачивание данных с proverki.gov.ru')
    parser.add_argument('--federal-law-248', action='store_true', help='Скачивать данные по 248-ФЗ')
    parser.add_argument('--start-year', type=int, help='Год начала скачивания')
    parser.add_argument('--start-month', type=int, help='Месяц начала скачивания')
    args = parser.parse_args()

    # Задаем начальную и конечную даты
    start_year = args.start_year if args.start_year is not None else 2021
    start_month = args.start_month if args.start_month is not None else 1
    end_year = 2025
    end_month = 5

    # Скачиваем файлы за указанный период
    current_year = start_year
    current_month = start_month
    successful_downloads = 0
    failed_downloads = 0
    total_files = (end_year - start_year) * 12 + (end_month - start_month + 1)
    current_file = 0

    print(f"\nВсего файлов для скачивания: {total_files}")
    print(f"Период: {start_year}/{start_month:02d} - {end_year}/{end_month:02d}")
    print(f"Режим: {'248-ФЗ' if args.federal_law_248 else 'Обычный'}")
    print("-" * 50)

    try:
        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            current_file += 1
            print(f"\nФайл {current_file} из {total_files}")
            print(f"Скачивание файла за {current_year}/{current_month:02d}")
            
            if download_file(current_year, current_month, args.federal_law_248):
                successful_downloads += 1
                print(f"✓ Успешно скачан файл за {current_year}/{current_month:02d}")
            else:
                failed_downloads += 1
                print(f"✗ Не удалось скачать файл за {current_year}/{current_month:02d}")
            
            # Переходим к следующему месяцу
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
                
            time.sleep(5)  # Пауза между запросами
                
    except KeyboardInterrupt:
        print("\n\nСкачивание прервано пользователем")
        print(f"Последний обработанный файл: {current_year}/{current_month:02d}")
        print(f"Для продолжения используйте команду:")
        print(f"python download_data.py {'--federal-law-248 ' if args.federal_law_248 else ''}--start-year {current_year} --start-month {current_month}")
        return

    print("\n" + "=" * 50)
    print("Скачивание завершено!")
    print(f"Успешно скачано: {successful_downloads} файлов")
    print(f"Не удалось скачать: {failed_downloads} файлов")
    print(f"Всего обработано: {current_file} файлов")
    print("=" * 50)

if __name__ == "__main__":
    main() 