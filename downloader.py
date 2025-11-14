#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import requests
import os
import logging
from datetime import datetime, timezone
import config
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('downloader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def create_html_storage_dir():
    """Создает директорию для хранения HTML файлов"""
    if not os.path.exists(config.HTML_STORAGE_DIR):
        os.makedirs(config.HTML_STORAGE_DIR)
        logger.info(f"Создана директория для HTML файлов: {config.HTML_STORAGE_DIR}")


def get_html_file_path(test_id):
    """Возвращает путь к HTML файлу для указанного test_id"""
    return os.path.join(config.HTML_STORAGE_DIR, f"test_{test_id}.html")


def get_metadata_file_path():
    """Возвращает путь к файлу метаданных"""
    return os.path.join(config.HTML_STORAGE_DIR, "download_metadata.json")


def load_download_metadata():
    """Загружает метаданные о скачанных файлах"""
    metadata_path = get_metadata_file_path()
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки метаданных: {e}")
    
    return {
        'downloaded': {},
        'failed': {},
        'last_processed': config.START_ID - 1,
        'total_downloaded': 0,
        'total_failed': 0
    }


def save_download_metadata(metadata):
    """Сохраняет метаданные о скачанных файлах"""
    metadata_path = get_metadata_file_path()
    try:
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения метаданных: {e}")


def fetch_test_page(session, test_id):
    """Скачивает HTML страницу теста"""
    url = f"https://zin.pw/cdz/test/{test_id}"
    headers = {
        "User-Agent": config.USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://zin.pw/",
        "Connection": "keep-alive",
        "Cookie": config.COOKIE
    }
    return session.get(url, headers=headers, timeout=30, allow_redirects=True)


def save_html_file(test_id, html_content, status_code):
    """Сохраняет HTML файл на диск"""
    file_path = get_html_file_path(test_id)
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения HTML файла для теста {test_id}: {e}")
        return False


def is_file_already_downloaded(test_id):
    """Проверяет, был ли файл уже скачан"""
    file_path = get_html_file_path(test_id)
    return os.path.exists(file_path) and os.path.getsize(file_path) > 0


def main():
    """Основная функция скачивания HTML файлов"""
    logger.info("Запуск скачивания HTML файлов...")
    
    # Создаем директорию для HTML файлов
    create_html_storage_dir()
    
    # Загружаем метаданные
    metadata = load_download_metadata()
    
    # Создаем сессию
    session = requests.Session()
    session.max_redirects = 30
    
    # Счетчики
    downloaded_count = 0
    error_count = 0
    skipped_count = 0
    
    start_id = max(metadata['last_processed'] + 1, config.START_ID)
    logger.info(f"Начинаем скачивание с ID {start_id} до {config.END_ID}")
    logger.info(f"Ранее скачано: {metadata['total_downloaded']}, ошибок: {metadata['total_failed']}")
    
    try:
        for test_id in range(start_id, config.END_ID + 1):
            # Проверяем, не скачан ли уже файл
            if is_file_already_downloaded(test_id):
                skipped_count += 1
                if test_id % 1000 == 0:
                    logger.info(f"Пропущен уже скачанный файл: {test_id}")
                continue
            
            try:
                # Скачиваем страницу
                resp = fetch_test_page(session, test_id)
                
                if resp.status_code == 200:
                    # Сохраняем HTML файл
                    if save_html_file(test_id, resp.text, resp.status_code):
                        downloaded_count += 1
                        metadata['downloaded'][str(test_id)] = {
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                            'status_code': resp.status_code,
                            'content_length': len(resp.text)
                        }
                        
                        if test_id % 100 == 0:
                            logger.info(f"Скачан тест {test_id}, размер: {len(resp.text)} символов")
                    else:
                        error_count += 1
                        metadata['failed'][str(test_id)] = {
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                            'error': 'Failed to save file',
                            'status_code': resp.status_code
                        }
                else:
                    error_count += 1
                    logger.warning(f"HTTP {resp.status_code} для теста {test_id}")
                    metadata['failed'][str(test_id)] = {
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'error': f'HTTP {resp.status_code}',
                        'status_code': resp.status_code
                    }
                    
                    # Увеличиваем задержку при ошибках сервера
                    if resp.status_code in (429, 503, 502, 500):
                        time.sleep(5)
            
            except Exception as e:
                error_count += 1
                logger.error(f"Ошибка скачивания теста {test_id}: {e}")
                metadata['failed'][str(test_id)] = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'error': str(e),
                    'status_code': None
                }
                time.sleep(2)
            
            # Обновляем метаданные
            metadata['last_processed'] = test_id
            metadata['total_downloaded'] = len(metadata['downloaded'])
            metadata['total_failed'] = len(metadata['failed'])
            
            # Сохраняем метаданные каждые 100 файлов
            if test_id % 100 == 0:
                save_download_metadata(metadata)
                logger.info(f"Прогресс: {test_id}/{config.END_ID}, "
                          f"скачано: {downloaded_count}, ошибок: {error_count}, пропущено: {skipped_count}")
            
            # Задержка между запросами
            time.sleep(config.SLEEP_BETWEEN)
    
    except KeyboardInterrupt:
        logger.info("Скачивание прервано пользователем")
    
    finally:
        # Сохраняем финальные метаданные
        save_download_metadata(metadata)
        
        total_downloaded = metadata['total_downloaded']
        total_failed = metadata['total_failed']
        
        logger.info(f"Скачивание завершено:")
        logger.info(f"  - Всего скачано: {total_downloaded}")
        logger.info(f"  - Всего ошибок: {total_failed}")
        logger.info(f"  - В этой сессии скачано: {downloaded_count}")
        logger.info(f"  - В этой сессии ошибок: {error_count}")
        logger.info(f"  - Пропущено (уже скачано): {skipped_count}")
        logger.info(f"  - Последний обработанный ID: {metadata['last_processed']}")


if __name__ == "__main__":
    main()
