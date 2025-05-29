import os
import zipfile
from pathlib import Path
from tqdm import tqdm

def get_archive_size(zip_path):
    """Получает суммарный размер файлов в архиве"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            total_size = sum(info.file_size for info in zip_ref.infolist())
            return total_size
    except zipfile.BadZipFile:
        print(f"Ошибка: {zip_path} - поврежденный архив")
        return 0
    except Exception as e:
        print(f"Ошибка при обработке {zip_path}: {str(e)}")
        return 0

def format_size(size_bytes):
    """Форматирует размер в байтах в читаемый вид"""
    for unit in ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} ПБ"

def main():
    # Путь к директории с XML файлами
    xml_dir = "xml"
    
    # Счетчики
    total_archives = 0
    total_size = 0
    total_uncompressed_size = 0
    
    # Словарь для хранения статистики по директориям
    dir_stats = {}
    
    print("Поиск и анализ архивов...")
    
    # Рекурсивно обходим все директории
    for root, dirs, files in os.walk(xml_dir):
        # Ищем ZIP файлы
        zip_files = [f for f in files if f.endswith('.zip')]
        
        if zip_files:
            dir_path = os.path.relpath(root, xml_dir)
            dir_stats[dir_path] = {
                'archives': 0,
                'size': 0,
                'uncompressed_size': 0
            }
            
            print(f"\nДиректория: {dir_path}")
            
            # Обрабатываем каждый архив
            for zip_file in tqdm(zip_files, desc="Обработка архивов"):
                zip_path = os.path.join(root, zip_file)
                
                # Получаем размер архива
                archive_size = os.path.getsize(zip_path)
                
                # Получаем суммарный размер файлов в архиве
                uncompressed_size = get_archive_size(zip_path)
                
                # Обновляем статистику
                total_archives += 1
                total_size += archive_size
                total_uncompressed_size += uncompressed_size
                
                dir_stats[dir_path]['archives'] += 1
                dir_stats[dir_path]['size'] += archive_size
                dir_stats[dir_path]['uncompressed_size'] += uncompressed_size
    
    # Выводим общую статистику
    print("\n" + "="*80)
    print("ОБЩАЯ СТАТИСТИКА:")
    print("="*80)
    print(f"Всего архивов: {total_archives}")
    print(f"Общий размер архивов: {format_size(total_size)}")
    print(f"Суммарный размер файлов в архивах: {format_size(total_uncompressed_size)}")
    print(f"Средний коэффициент сжатия: {total_uncompressed_size/total_size:.2f}x")
    
    # Выводим статистику по директориям
    print("\n" + "="*80)
    print("СТАТИСТИКА ПО ДИРЕКТОРИЯМ:")
    print("="*80)
    
    for dir_path, stats in dir_stats.items():
        print(f"\nДиректория: {dir_path}")
        print(f"Количество архивов: {stats['archives']}")
        print(f"Размер архивов: {format_size(stats['size'])}")
        print(f"Суммарный размер файлов в архивах: {format_size(stats['uncompressed_size'])}")
        if stats['size'] > 0:
            print(f"Коэффициент сжатия: {stats['uncompressed_size']/stats['size']:.2f}x")

if __name__ == "__main__":
    main() 