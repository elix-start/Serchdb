#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from bs4 import BeautifulSoup
import config

def debug_test_html(test_id):
    """Отладка парсинга конкретного теста"""
    file_path = os.path.join(config.HTML_STORAGE_DIR, f"test_{test_id}.html")
    
    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        return
    
    with open(file_path, 'r', encoding='utf-8') as f:
        html = f.read()
    
    soup = BeautifulSoup(html, "html.parser")
    
    print(f"=== ОТЛАДКА ТЕСТА {test_id} ===")
    print(f"Размер HTML: {len(html)} символов")
    
    # Ищем все h1 с текстом "Задание"
    h1_tasks = soup.find_all("h1")
    print(f"Всего h1 тегов: {len(h1_tasks)}")
    
    task_h1s = [h1 for h1 in h1_tasks if "Задание" in h1.get_text()]
    print(f"h1 с 'Задание': {len(task_h1s)}")
    
    for i, h1 in enumerate(task_h1s[:5]):  # Показываем первые 5
        print(f"  {i+1}. {h1.get_text().strip()}")
        print(f"     Классы: {h1.get('class', [])}")
    
    # Ищем другие возможные структуры
    print("\n=== ПОИСК ДРУГИХ СТРУКТУР ===")
    
    # Ищем div с заданиями
    task_divs = soup.find_all("div", string=lambda text: text and "Задание" in text)
    print(f"div с 'Задание': {len(task_divs)}")
    
    # Ищем span с заданиями
    task_spans = soup.find_all("span", string=lambda text: text and "Задание" in text)
    print(f"span с 'Задание': {len(task_spans)}")
    
    # Ищем по классам
    common_classes = [
        "text-xl leading-7 text-primary",
        "task-title",
        "question-title",
        "exercise-title"
    ]
    
    for class_name in common_classes:
        elements = soup.find_all(class_=class_name)
        print(f"Элементы с классом '{class_name}': {len(elements)}")
        if elements:
            for i, elem in enumerate(elements[:3]):
                print(f"  {i+1}. {elem.get_text().strip()[:100]}...")
    
    # Ищем input поля (ответы)
    inputs = soup.find_all("input")
    print(f"\nВсего input полей: {len(inputs)}")
    
    text_inputs = soup.find_all("input", {"type": "text"})
    print(f"input type='text': {len(text_inputs)}")
    
    checked_inputs = soup.find_all("input", {"checked": True})
    print(f"Отмеченные input: {len(checked_inputs)}")
    
    # Ищем элементы с data-selected
    selected_elements = soup.find_all(attrs={"data-selected": "true"})
    print(f"Элементы с data-selected='true': {len(selected_elements)}")
    
    print("\n=== ПРИМЕРЫ НАЙДЕННЫХ ЭЛЕМЕНТОВ ===")
    if text_inputs:
        print("Примеры text input:")
        for i, inp in enumerate(text_inputs[:3]):
            print(f"  {i+1}. value='{inp.get('value', '')}' name='{inp.get('name', '')}'")
    
    if selected_elements:
        print("Примеры selected элементов:")
        for i, elem in enumerate(selected_elements[:3]):
            print(f"  {i+1}. {elem.name}: {elem.get_text().strip()[:100]}...")

if __name__ == "__main__":
    # Тестируем несколько тестов
    test_ids = [89247, 88316, 88320, 88389]  # 1, 12, 17, 40 вопросов соответственно
    
    for test_id in test_ids:
        debug_test_html(test_id)
        print("=" * 80)
