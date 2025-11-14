#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sqlite3
import logging
import re
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import config
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('html_parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def init_db(conn):
    """Инициализация базы данных с обновленной схемой"""
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tests (
        test_id INTEGER,
        question TEXT,
        answer TEXT,
        raw_html TEXT,
        html_file_path TEXT,
        fetched_at TEXT,
        parsed_at TEXT,
        question_idx INTEGER DEFAULT 0,
        PRIMARY KEY (test_id, question_idx)
    )
    """)
    
    # Добавляем новые колонки, если их нет
    try:
        cur.execute("ALTER TABLE tests ADD COLUMN html_file_path TEXT")
    except sqlite3.OperationalError:
        pass  # Колонка уже существует
    
    try:
        cur.execute("ALTER TABLE tests ADD COLUMN parsed_at TEXT")
    except sqlite3.OperationalError:
        pass  # Колонка уже существует
    
    conn.commit()


def parse_test_html(html):
    """Парсинг HTML содержимого теста"""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    
    # Ищем заголовки заданий с новой структурой
    for h1 in soup.find_all("h1", class_="text-xl leading-7 text-primary"):
        h1_text = h1.get_text(strip=True)
        if not h1_text.startswith("Задание"):
            continue
        
        # Находим контейнер задания
        task_container = h1.find_parent("div")
        if not task_container:
            continue
        
        question = ""
        answer = ""
        
        # Извлекаем вопрос из параграфа с классом
        question_p = task_container.find("p", class_="leading-7 whitespace-pre-wrap my-4")
        if question_p:
            question = question_p.get_text(" ", strip=True)
        
        # Проверяем тип задания
        is_matching_task = False
        
        # 1. Ищем Next.js RSC данные с правильными ответами
        scripts = soup.find_all("script")
        json_answer = None
        
        for script in scripts:
            if script.string and "self.__next_f.push" in script.string:
                try:
                    # Ищем все self.__next_f.push вызовы
                    push_calls = re.findall(r'self\.__next_f\.push\(\[1,"([^"]+)"\]\)', script.string)
                    
                    for call_data in push_calls:
                        # Декодируем экранированные символы
                        decoded_data = call_data.replace('\\"', '"').replace('\\\\', '\\')
                        
                        if '"answer":' in decoded_data and '"right_answer":' in decoded_data:
                            # Ищем JSON объект с ответами
                            answer_match = re.search(r'"answer":\s*({[^}]*"right_answer"[^}]*})', decoded_data)
                            if answer_match:
                                json_str = answer_match.group(1)
                                
                                # Пытаемся найти полный JSON объект
                                brace_count = 0
                                end_pos = 0
                                for i, char in enumerate(json_str):
                                    if char == '{':
                                        brace_count += 1
                                    elif char == '}':
                                        brace_count -= 1
                                        if brace_count == 0:
                                            end_pos = i + 1
                                            break
                                
                                if end_pos > 0:
                                    json_str = json_str[:end_pos]
                                    answer_data = json.loads(json_str)
                                    json_answer = answer_data
                                    is_matching_task = True
                                    break
                except Exception as e:
                    continue
                
                if json_answer:
                    break
        
        # Если нашли JSON ответы, парсим их
        if json_answer and "right_answer" in json_answer:
            try:
                groups = json_answer["right_answer"]["groups"]
                options = {opt["id"]: opt["text"] for opt in json_answer["options"]}
                
                matching_pairs = []
                for group in groups:
                    group_id = group["group_id"]
                    group_name = options.get(group_id, f"Группа {group_id[:8]}")
                    
                    for option_id in group["options_ids"]:
                        option_name = options.get(option_id, f"Элемент {option_id[:8]}")
                        matching_pairs.append(f"{group_name}: {option_name}")
                
                if matching_pairs:
                    answer = " | ".join(matching_pairs)
            except Exception as e:
                # Если не удалось распарсить JSON, используем старый метод
                is_matching_task = False
        
        # 2. Если не нашли JSON, ищем задания на соотнесение (accordion)
        if not is_matching_task:
            accordion_sections = task_container.find_all("div", {"data-slot": "base"})
            if accordion_sections and len(accordion_sections) > 1:
                is_matching_task = True
                matching_pairs = []
                
                for section in accordion_sections:
                    # Извлекаем категорию из заголовка
                    category_elem = section.find("span", {"data-slot": "subtitle"})
                    if category_elem:
                        category = category_elem.get_text(strip=True)
                        
                        # Ищем элементы внутри этой категории
                        content_div = section.find("div", {"data-slot": "content"})
                        if content_div:
                            # Ищем изображения
                            images = content_div.find_all("div", string=lambda text: text and text.endswith(".jpg"))
                            for img in images:
                                img_name = img.get_text(strip=True)
                                matching_pairs.append(f"{category}: {img_name}")
                            
                            # Ищем аудио файлы
                            audios = content_div.find_all("audio")
                            for audio in audios:
                                src = audio.get("src", "")
                                if src:
                                    audio_name = src.split("/")[-1] if "/" in src else src
                                    matching_pairs.append(f"{category}: {audio_name}")
                
                if matching_pairs:
                    answer = " | ".join(matching_pairs)
        
        # 2. Если не задание на соотнесение, ищем обычные ответы
        if not is_matching_task:
            # Извлекаем ответ из input поля
            answer_input = task_container.find("input", {"type": "text"})
            if answer_input and answer_input.get("value"):
                answer = answer_input.get("value").strip()
            
            # Если не нашли ответ в input, ищем в других местах
            if not answer:
                # Ищем выбранные элементы
                selected_elements = task_container.find_all(attrs={"data-selected": "true"})
                if selected_elements:
                    answers = []
                    for elem in selected_elements:
                        text = elem.get_text(" ", strip=True)
                        if text:
                            answers.append(text)
                    answer = " | ".join(answers) if answers else ""
                else:
                    # Ищем отмеченные чекбоксы/радиокнопки
                    checked_inputs = task_container.find_all("input", {"checked": True})
                    if checked_inputs:
                        answers = []
                        for checked_input in checked_inputs:
                            label = checked_input.find_parent("label")
                            if label:
                                text = label.get_text(" ", strip=True)
                                if text:
                                    answers.append(text)
                        answer = " | ".join(answers) if answers else ""
        
        # Добавляем результат только если есть вопрос
        if question:
            results.append({"question": question, "answer": answer})
    
    # Если не нашли задания с новой структурой, пробуем старую
    if not results:
        for h1 in soup.find_all("h1"):
            if not h1.get_text(strip=True).startswith("Задание"):
                continue
            
            parent = h1.find_parent()
            question = ""
            answer = ""
            
            # Извлекаем вопрос
            p = parent.find("p")
            if p:
                question = p.get_text(" ", strip=True)
            
            # Извлекаем ответ
            selected_elements = parent.find_all(attrs={"data-selected": "true"})
            if selected_elements:
                answers = []
                for elem in selected_elements:
                    text = elem.get_text(" ", strip=True)
                    if text:
                        answers.append(text)
                answer = " | ".join(answers) if answers else ""
            else:
                checked_inputs = parent.find_all("input", attrs={"checked": True})
                if checked_inputs:
                    answers = []
                    for checked_input in checked_inputs:
                        lbl = checked_input.find_parent("label")
                        if lbl:
                            text = lbl.get_text(" ", strip=True)
                            if text:
                                answers.append(text)
                    answer = " | ".join(answers) if answers else ""
            
            if question:
                results.append({"question": question, "answer": answer})
    
    return results


def save_test_to_db(conn, test_id, questions_answers, raw_html, html_file_path):
    """Сохранение теста в базу данных (без raw_html)"""
    cur = conn.cursor()
    parsed_at = datetime.now(timezone.utc).isoformat()
    
    if questions_answers:
        for idx, qa in enumerate(questions_answers):
            cur.execute("""
                INSERT OR REPLACE INTO tests 
                (test_id, question, answer, html_file_path, parsed_at, question_idx)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (test_id, qa["question"], qa["answer"], html_file_path, parsed_at, idx))
    else:
        # Сохраняем пустую запись, если вопросы не найдены
        cur.execute("""
            INSERT OR REPLACE INTO tests 
            (test_id, question, answer, html_file_path, parsed_at, question_idx)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (test_id, "", "", html_file_path, parsed_at, 0))
    
    conn.commit()


def get_html_file_path(test_id):
    """Возвращает путь к HTML файлу для указанного test_id"""
    return os.path.join(config.HTML_STORAGE_DIR, f"test_{test_id}.html")


def load_html_file(test_id):
    """Загружает HTML файл с диска"""
    file_path = get_html_file_path(test_id)
    
    if not os.path.exists(file_path):
        return None, None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content, file_path
    except Exception as e:
        logger.error(f"Ошибка чтения HTML файла для теста {test_id}: {e}")
        return None, None


def get_parsing_progress():
    """Получает прогресс парсинга из базы данных"""
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur = conn.cursor()
        
        # Получаем максимальный обработанный test_id
        cur.execute("SELECT MAX(test_id) FROM tests WHERE parsed_at IS NOT NULL")
        result = cur.fetchone()
        last_parsed = result[0] if result[0] is not None else config.START_ID - 1
        
        # Получаем общее количество обработанных тестов
        cur.execute("SELECT COUNT(DISTINCT test_id) FROM tests WHERE parsed_at IS NOT NULL")
        total_parsed = cur.fetchone()[0]
        
        conn.close()
        return last_parsed, total_parsed
        
    except Exception as e:
        logger.error(f"Ошибка получения прогресса парсинга: {e}")
        return config.START_ID - 1, 0


def get_available_html_files():
    """Получает список доступных HTML файлов"""
    if not os.path.exists(config.HTML_STORAGE_DIR):
        logger.error(f"Директория {config.HTML_STORAGE_DIR} не существует")
        return []
    
    html_files = []
    for filename in os.listdir(config.HTML_STORAGE_DIR):
        if filename.startswith("test_") and filename.endswith(".html"):
            try:
                test_id = int(filename.replace("test_", "").replace(".html", ""))
                html_files.append(test_id)
            except ValueError:
                continue
    
    return sorted(html_files)


def is_test_already_parsed(conn, test_id):
    """Проверяет, был ли тест уже обработан"""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM tests WHERE test_id = ? AND parsed_at IS NOT NULL", (test_id,))
    return cur.fetchone()[0] > 0


def main():
    """Основная функция парсинга HTML файлов в базу данных"""
    logger.info("Запуск парсинга HTML файлов в базу данных...")
    
    # Проверяем существование директории с HTML файлами
    if not os.path.exists(config.HTML_STORAGE_DIR):
        logger.error(f"Директория {config.HTML_STORAGE_DIR} не существует. Сначала запустите downloader.py")
        return
    
    # Подключаемся к базе данных
    try:
        conn = sqlite3.connect(config.DB_PATH)
        init_db(conn)
        logger.info(f"Подключение к БД: {config.DB_PATH}")
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        return
    
    # Получаем список доступных HTML файлов
    available_files = get_available_html_files()
    logger.info(f"Найдено HTML файлов: {len(available_files)}")
    
    if not available_files:
        logger.warning("HTML файлы не найдены. Сначала запустите downloader.py")
        conn.close()
        return
    
    # Получаем прогресс парсинга
    last_parsed, total_parsed = get_parsing_progress()
    logger.info(f"Ранее обработано тестов: {total_parsed}, последний ID: {last_parsed}")
    
    # Счетчики
    parsed_count = 0
    error_count = 0
    skipped_count = 0
    
    try:
        for test_id in available_files:
            # Пропускаем уже обработанные тесты
            if is_test_already_parsed(conn, test_id):
                skipped_count += 1
                if test_id % 1000 == 0:
                    logger.info(f"Пропущен уже обработанный тест: {test_id}")
                continue
            
            try:
                # Загружаем HTML файл
                html_content, file_path = load_html_file(test_id)
                
                if html_content is None:
                    error_count += 1
                    logger.error(f"Не удалось загрузить HTML файл для теста {test_id}")
                    continue
                
                # Парсим HTML
                questions_answers = parse_test_html(html_content)
                
                # Сохраняем в базу данных
                save_test_to_db(conn, test_id, questions_answers, html_content, file_path)
                parsed_count += 1
                
                if questions_answers:
                    logger.info(f"Тест {test_id}: обработано {len(questions_answers)} вопросов")
                else:
                    logger.warning(f"Тест {test_id}: вопросы не найдены")
                
                # Прогресс каждые 100 тестов
                if parsed_count % 100 == 0:
                    logger.info(f"Прогресс: обработано {parsed_count} тестов, ошибок: {error_count}, пропущено: {skipped_count}")
            
            except Exception as e:
                error_count += 1
                logger.error(f"Ошибка обработки теста {test_id}: {e}")
    
    except KeyboardInterrupt:
        logger.info("Парсинг прерван пользователем")
    
    finally:
        conn.close()
        
        logger.info(f"Парсинг завершен:")
        logger.info(f"  - Обработано тестов: {parsed_count}")
        logger.info(f"  - Ошибок: {error_count}")
        logger.info(f"  - Пропущено (уже обработано): {skipped_count}")


if __name__ == "__main__":
    main()
