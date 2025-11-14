#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import logging
import os
from typing import List, Tuple, Optional
import config

logger = logging.getLogger(__name__)

class ZinDatabase:
    """Класс для работы с базой данных ЦДЗ"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DB_PATH
    
    def get_connection(self) -> sqlite3.Connection:
        """Получить соединение с базой данных"""
        return sqlite3.connect(self.db_path)
    
    def search_questions(self, query: str, limit: int = 20) -> List[Tuple]:
        """
        Поиск вопросов и ответов по тексту
        
        Args:
            query: Поисковый запрос
            limit: Максимальное количество результатов
            
        Returns:
            List[Tuple]: Список кортежей (test_id, question, answer, question_idx, html_file_path)
        """
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                
                search_query = f"%{query.lower()}%"
                cur.execute("""
                    SELECT test_id, question, answer, question_idx, html_file_path
                    FROM tests 
                    WHERE (LOWER(question) LIKE ? OR LOWER(answer) LIKE ?)
                    AND question != ''
                    ORDER BY 
                        CASE 
                            WHEN LOWER(question) LIKE ? THEN 1
                            WHEN LOWER(answer) LIKE ? THEN 2
                            ELSE 3
                        END,
                        test_id
                    LIMIT ?
                """, (search_query, search_query, search_query, search_query, limit))
                
                return cur.fetchall()
                
        except Exception as e:
            logger.error(f"Ошибка поиска в БД: {e}")
            return []
    
    def get_test_by_id(self, test_id: int) -> List[Tuple]:
        """
        Получить все вопросы конкретного теста
        
        Args:
            test_id: ID теста
            
        Returns:
            List[Tuple]: Список вопросов теста (test_id, question, answer, question_idx, html_file_path)
        """
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT test_id, question, answer, question_idx, html_file_path
                    FROM tests 
                    WHERE test_id = ? AND question != ''
                    ORDER BY question_idx
                """, (test_id,))
                
                return cur.fetchall()
                
        except Exception as e:
            logger.error(f"Ошибка получения теста {test_id}: {e}")
            return []
    
    def get_random_questions(self, count: int = 5) -> List[Tuple]:
        """
        Получить случайные вопросы
        
        Args:
            count: Количество вопросов
            
        Returns:
            List[Tuple]: Список случайных вопросов (test_id, question, answer, question_idx, html_file_path)
        """
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT test_id, question, answer, question_idx, html_file_path
                    FROM tests 
                    WHERE question != ''
                    ORDER BY RANDOM()
                    LIMIT ?
                """, (count,))
                
                return cur.fetchall()
                
        except Exception as e:
            logger.error(f"Ошибка получения случайных вопросов: {e}")
            return []
    
    def get_statistics(self) -> dict:
        """
        Получить статистику базы данных
        
        Returns:
            dict: Словарь со статистикой
        """
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                
                # Общее количество записей
                cur.execute("SELECT COUNT(*) FROM tests")
                total_count = cur.fetchone()[0]
                
                # Количество уникальных тестов
                cur.execute("SELECT COUNT(DISTINCT test_id) FROM tests")
                unique_tests = cur.fetchone()[0]
                
                # Количество записей с вопросами
                cur.execute("SELECT COUNT(*) FROM tests WHERE question != ''")
                with_questions = cur.fetchone()[0]
                
                # Последний добавленный тест
                cur.execute("SELECT MAX(test_id) FROM tests")
                last_test_id = cur.fetchone()[0] or 0
                
                return {
                    'total_records': total_count,
                    'unique_tests': unique_tests,
                    'records_with_questions': with_questions,
                    'last_test_id': last_test_id,
                    'fill_percentage': (with_questions / total_count * 100) if total_count > 0 else 0
                }
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}
    
    def search_by_keywords(self, keywords: List[str], limit: int = 20) -> List[Tuple]:
        """
        Поиск по нескольким ключевым словам
        
        Args:
            keywords: Список ключевых слов
            limit: Максимальное количество результатов
            
        Returns:
            List[Tuple]: Список результатов поиска
        """
        if not keywords:
            return []
        
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                
                # Создаем условия для каждого ключевого слова
                conditions = []
                params = []
                
                for keyword in keywords:
                    keyword_pattern = f"%{keyword.lower()}%"
                    conditions.append("(LOWER(question) LIKE ? OR LOWER(answer) LIKE ?)")
                    params.extend([keyword_pattern, keyword_pattern])
                
                where_clause = " AND ".join(conditions)
                params.append(limit)
                
                query = f"""
                    SELECT test_id, question, answer, question_idx, html_file_path
                    FROM tests 
                    WHERE {where_clause}
                    AND question != ''
                    ORDER BY test_id
                    LIMIT ?
                """
                
                cur.execute(query, params)
                return cur.fetchall()
                
        except Exception as e:
            logger.error(f"Ошибка поиска по ключевым словам: {e}")
            return []
    
    def search_by_any_keywords(self, keywords: List[str], limit: int = 20) -> List[Tuple]:
        """
        Поиск по любому из ключевых слов (OR) с простым ранжированием по количеству совпадений
        
        Args:
            keywords: Список ключевых слов
            limit: Максимальное количество результатов
        
        Returns:
            List[Tuple]: Список результатов поиска
        """
        if not keywords:
            return []
        
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                
                # Формируем выражение подсчета совпадений и условия OR
                score_parts = []
                where_parts = []
                params = []
                for kw in keywords:
                    pattern = f"%{kw.lower()}%"
                    score_parts.append("(CASE WHEN LOWER(question) LIKE ? OR LOWER(answer) LIKE ? THEN 1 ELSE 0 END)")
                    params.extend([pattern, pattern])
                    where_parts.append("LOWER(question) LIKE ? OR LOWER(answer) LIKE ?")
                # Параметры для WHERE (OR)
                for kw in keywords:
                    pattern = f"%{kw.lower()}%"
                    params.extend([pattern, pattern])
                
                score_expr = " + ".join(score_parts) if score_parts else "0"
                where_clause = "(" + " OR ".join(where_parts) + ")" if where_parts else "1=1"
                
                params.append(limit)
                
                query = f"""
                    SELECT test_id, question, answer, question_idx, html_file_path,
                           {score_expr} AS score
                    FROM tests
                    WHERE {where_clause}
                      AND question != ''
                    ORDER BY score DESC, test_id
                    LIMIT ?
                """
                
                cur.execute(query, params)
                rows = cur.fetchall()
                # Возвращаем без поля score
                return [row[:5] for row in rows]
        except Exception as e:
            logger.error(f"Ошибка OR-поиска по ключевым словам: {e}")
            return []
    
    def get_tests_count_by_date(self) -> List[Tuple]:
        """
        Получить количество тестов по датам добавления
        
        Returns:
            List[Tuple]: Список (дата, количество)
        """
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT DATE(fetched_at) as date, COUNT(DISTINCT test_id) as count
                    FROM tests 
                    WHERE fetched_at IS NOT NULL
                    GROUP BY DATE(fetched_at)
                    ORDER BY date DESC
                    LIMIT 30
                """)
                
                return cur.fetchall()
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики по датам: {e}")
            return []
    
    def get_test_html_content(self, test_id: int) -> Optional[str]:
        """
        Получить HTML содержимое теста из файла
        
        Args:
            test_id: ID теста
            
        Returns:
            Optional[str]: HTML содержимое теста или None
        """
        try:
            # Сначала пробуем получить путь к файлу из БД
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT html_file_path
                    FROM tests 
                    WHERE test_id = ? AND html_file_path IS NOT NULL
                    LIMIT 1
                """, (test_id,))
                
                result = cur.fetchone()
                html_file_path = result[0] if result else None
            
            # Если путь не найден в БД, формируем стандартный путь
            if not html_file_path:
                html_file_path = os.path.join(config.HTML_STORAGE_DIR, f"test_{test_id}.html")
            
            # Читаем файл
            if os.path.exists(html_file_path):
                with open(html_file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                logger.warning(f"HTML файл не найден: {html_file_path}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка получения HTML для теста {test_id}: {e}")
            return None

    def get_test_html_file_path(self, test_id: int) -> Optional[str]:
        """
        Получить путь к HTML файлу теста, если существует.
        """
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT html_file_path
                    FROM tests
                    WHERE test_id = ? AND html_file_path IS NOT NULL
                    LIMIT 1
                    """,
                    (test_id,),
                )
                result = cur.fetchone()
                html_file_path = result[0] if result else None
            if not html_file_path:
                html_file_path = os.path.join(config.HTML_STORAGE_DIR, f"test_{test_id}.html")
            if os.path.exists(html_file_path):
                return html_file_path
            logger.warning(f"HTML файл не найден: {html_file_path}")
            return None
        except Exception as e:
            logger.error(f"Ошибка получения пути к HTML для теста {test_id}: {e}")
            return None

# Создаем глобальный экземпляр для удобства
db = ZinDatabase()
