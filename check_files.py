import zipfile
import os
import hashlib
from collections import defaultdict

def calculate_file_hash(filename):
    """Вычисляет MD5 хеш файла"""
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def check_zip_contents(zip_path):
    """Проверяет содержимое zip-файла и возвращает информацию о файлах внутри"""
    contents = {}
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                contents[file_info.filename] = {
                    'size': file_info.file_size,
                    'crc': file_info.CRC
                }
    except zipfile.BadZipFile:
        print(f"Ошибка: {zip_path} не является корректным zip-файлом")
        return None
    return contents

def main():
    # Список файлов для проверки
    files = [
        'data/248/data-20210126-structure-20220125.zip',
        'data/248/data-20210226-structure-20220125.zip',
        'data/248/data-20210326-structure-20220125.zip'
    ]
    
    # Проверяем существование файлов
    existing_files = [f for f in files if os.path.exists(f)]
    if not existing_files:
        print("Файлы не найдены!")
        return
    
    print("Проверка файлов:")
    print("-" * 50)
    
    # Собираем информацию о файлах
    file_info = {}
    for filename in existing_files:
        print(f"\nПроверка файла: {filename}")
        print(f"Размер файла: {os.path.getsize(filename):,} байт")
        print(f"MD5 хеш: {calculate_file_hash(filename)}")
        
        contents = check_zip_contents(filename)
        if contents:
            file_info[filename] = contents
            print(f"Количество файлов в архиве: {len(contents)}")
            print("Первые 5 файлов в архиве:")
            for i, (name, info) in enumerate(list(contents.items())[:5]):
                print(f"  {name}: {info['size']:,} байт, CRC: {info['crc']}")
    
    print("\nСравнение содержимого:")
    print("-" * 50)
    
    # Сравниваем содержимое файлов
    if len(file_info) > 1:
        # Получаем список всех файлов из всех архивов
        all_files = set()
        for contents in file_info.values():
            all_files.update(contents.keys())
        
        # Создаем словарь для хранения информации о различиях
        differences = defaultdict(dict)
        
        # Проверяем каждый файл
        for filename in all_files:
            sizes = set()
            crcs = set()
            
            for zip_name, contents in file_info.items():
                if filename in contents:
                    sizes.add(contents[filename]['size'])
                    crcs.add(contents[filename]['crc'])
            
            if len(sizes) > 1 or len(crcs) > 1:
                differences[filename] = {
                    'sizes': sizes,
                    'crcs': crcs
                }
        
        if differences:
            print("\nНайдены различия в следующих файлах:")
            for filename, diff in differences.items():
                print(f"\n{filename}:")
                print(f"  Размеры: {diff['sizes']}")
                print(f"  CRC: {diff['crcs']}")
        else:
            print("\nВсе файлы в архивах идентичны!")
    else:
        print("\nНедостаточно файлов для сравнения")

if __name__ == "__main__":
    main() 