import os
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
import shutil
from pathlib import Path
import json
from datetime import datetime
from tqdm import tqdm

def create_temp_dir():
    """Создает временную директорию для распаковки архивов"""
    temp_dir = Path("tmp")
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir()
    return temp_dir

def extract_archive(archive_path, temp_dir):
    """Распаковывает архив во временную директорию"""
    with zipfile.ZipFile(archive_path, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)

def analyze_xml_structure(xml_file):
    """Анализирует структуру XML файла и возвращает информацию о связях"""
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    # Словарь для хранения информации о структуре
    structure = {
        'elements': set(),
        'attributes': defaultdict(set),
        'relationships': defaultdict(set)
    }
    
    def process_element(element, parent=None):
        # Добавляем элемент в список
        structure['elements'].add(element.tag)
        
        # Обрабатываем атрибуты
        for attr_name, attr_value in element.attrib.items():
            structure['attributes'][element.tag].add(attr_name)
        
        # Обрабатываем связи между элементами
        if parent is not None:
            structure['relationships'][parent.tag].add(element.tag)
        
        # Рекурсивно обрабатываем дочерние элементы
        for child in element:
            process_element(child, element)
    
    process_element(root)
    return structure

def get_file_size(file_path):
    """Возвращает размер файла в байтах"""
    return file_path.stat().st_size

def has_new_information(structure, total_structure):
    """Проверяет, содержит ли структура новую информацию"""
    # Проверяем новые элементы
    new_elements = structure['elements'] - total_structure['elements']
    if new_elements:
        return True
    
    # Проверяем новые атрибуты
    for tag, attrs in structure['attributes'].items():
        if attrs - total_structure['attributes'][tag]:
            return True
    
    # Проверяем новые связи
    for parent, children in structure['relationships'].items():
        if children - total_structure['relationships'][parent]:
            return True
    
    return False

def save_results(total_structure, output_file):
    """Сохраняет результаты анализа в JSON файл"""
    # Преобразуем множества в списки для сериализации в JSON
    result = {
        'elements': sorted(list(total_structure['elements'])),
        'attributes': {tag: sorted(list(attrs)) for tag, attrs in total_structure['attributes'].items()},
        'relationships': {parent: sorted(list(children)) for parent, children in total_structure['relationships'].items()},
        'analysis_date': datetime.now().isoformat()
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

def main():
    xml_dir = Path("xml")
    temp_dir = create_temp_dir()
    output_file = f"xml_structure_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # Общая структура для всех файлов
    total_structure = {
        'elements': set(),
        'attributes': defaultdict(set),
        'relationships': defaultdict(set)
    }
    
    try:
        # Собираем все архивы для анализа
        archives = []
        
        print("Сбор архивов для анализа...")
        
        # Добавляем архивы из директории 248
        xml_248_dir = xml_dir / "248"
        if xml_248_dir.exists():
            archives.extend(xml_248_dir.glob("*.zip"))
        
        # Добавляем архивы из корневой директории
        archives.extend(xml_dir.glob("*.zip"))
        
        # Сортируем архивы по размеру
        archives.sort(key=get_file_size)
        
        # Анализируем архивы
        files_with_new_info = 0
        total_archives = len(archives)
        
        print(f"\nВсего архивов для анализа: {total_archives}")
        
        # Создаем прогресс-бар для архивов
        pbar = tqdm(archives, desc="Анализ архивов", unit="архив")
        
        for archive in pbar:
            pbar.set_postfix({
                'размер': f"{get_file_size(archive) / 1024:.1f}KB",
                'новых': files_with_new_info
            })
            
            # Создаем временную директорию для архива
            archive_temp_dir = temp_dir / archive.stem
            archive_temp_dir.mkdir(exist_ok=True)
            
            try:
                # Распаковываем архив
                extract_archive(archive, archive_temp_dir)
                
                # Анализируем XML файлы из архива
                archive_files = list(archive_temp_dir.rglob("*.xml"))
                archive_files.sort(key=get_file_size)
                
                for xml_file in archive_files:
                    structure = analyze_xml_structure(xml_file)
                    
                    if has_new_information(structure, total_structure):
                        files_with_new_info += 1
                        pbar.write(f"Найдена новая информация в файле: {xml_file.name} из архива {archive.name}")
                        
                        # Объединяем результаты
                        total_structure['elements'].update(structure['elements'])
                        for tag, attrs in structure['attributes'].items():
                            total_structure['attributes'][tag].update(attrs)
                        for parent, children in structure['relationships'].items():
                            total_structure['relationships'][parent].update(children)
                        
                        # Сохраняем промежуточные результаты
                        save_results(total_structure, output_file)
                
                # Если последние 5 файлов не добавили новой информации, останавливаемся
                if len(archive_files) >= 5 and files_with_new_info == 0:
                    pbar.write("\nПоследние 5 файлов не добавили новой информации. Останавливаем анализ.")
                    break
            
            finally:
                # Очищаем временную директорию архива
                shutil.rmtree(archive_temp_dir)
        
        pbar.close()
    
    finally:
        # Удаляем временную директорию
        shutil.rmtree(temp_dir)
    
    # Выводим итоговую сводку
    print("\nИтоговая сводка:")
    print(f"Проанализировано архивов: {total_archives}")
    print(f"Файлов с новой информацией: {files_with_new_info}")
    print(f"Всего уникальных элементов: {len(total_structure['elements'])}")
    print(f"Всего связей между элементами: {sum(len(children) for children in total_structure['relationships'].values())}")
    print(f"\nРезультаты анализа сохранены в файл: {output_file}")

if __name__ == "__main__":
    main() 