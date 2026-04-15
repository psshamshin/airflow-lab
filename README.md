# Лабораторная работа №2
## Оркестрирование задач обработки данных с использованием Apache Airflow

**Студент:** Павел, группа ИТБ(б)-О-23/1  
**Дисциплина:** Большие данные и технологии обработки  
**Инструмент:** Apache Airflow 2.x + PostgreSQL

---

## Цель работы

Научиться создавать системы работы с потоками данных с использованием инструмента Apache Airflow: развернуть приложение, разработать DAG и поставить его на выполнение.

---

## Описание предметной области

Разработана схема базы данных для **репетиторской платформы**, включающая:

| Таблица | Описание | Кол-во записей (тест) |
|---|---|---|
| `Subject` | Справочник учебных предметов | 10 |
| `Tutor` | Репетиторы с почасовой ставкой | 20 |
| `TutorSubject` | Связь репетитор ↔ предметы (M:N) | ~40 |
| `Student` | Ученики | 50 |
| `Lesson` | Занятия со статусом | 200 |
| `Payment` | Оплата завершённых занятий | ~150 |
| `Review` | Отзывы учеников (рейтинг 1–5) | ~110 |

### Схема базы данных

```
Subject ──< TutorSubject >── Tutor
                                │
Subject ──────────────── Lesson ─┤
                           │    Student
                    Payment│
                    Review─┘
```

Ключевые ограничения:
- Оплата и отзыв привязаны к конкретному занятию (1:1)
- Репетитор может вести несколько предметов (M:N через TutorSubject)
- Сумма оплаты рассчитывается автоматически: `ставка × длительность / 60`

---

## Описание DAG

Файл: `tutor_platform_dag.py`

### Граф задач

```
create_tables
      │
 ┌────┴─────┬──────────┐
load_subjects  load_tutors  load_students
               │
         load_tutor_subjects
               │
 ┌─────────────┴──────────────┐
 load_lessons (ждёт tutors + students + subjects)
               │
       ┌───────┴────────┐
  load_payments    load_reviews
```

### Описание задач

| Task ID | Описание |
|---|---|
| `create_tables` | Создание всех таблиц БД (DDL) с проверкой IF NOT EXISTS |
| `load_subjects` | Загрузка 10 учебных предметов по категориям |
| `load_tutors` | Генерация 20 репетиторов со случайными ставками (800–3000 руб/ч) |
| `load_tutor_subjects` | Привязка каждого репетитора к 1–3 предметам |
| `load_students` | Генерация 50 учеников с датами рождения |
| `load_lessons` | Генерация 200 занятий на 2024 год, статусы: completed / cancelled / scheduled |
| `load_payments` | Расчёт и загрузка оплат для завершённых занятий |
| `load_reviews` | Генерация отзывов для 75% завершённых занятий |

### Параметры DAG

```python
schedule_interval = "@once"   # запуск один раз вручную
start_date        = 2024-01-01
catchup           = False
retries           = 1
retry_delay       = 5 минут
```

---

## Требования

- Python 3.8+
- PostgreSQL 13+
- Apache Airflow 2.7+
- psycopg2

---

## Установка и запуск

### 1. Установка Python-зависимостей

```bash
pip install apache-airflow psycopg2-binary
```

Или с фиксированными версиями (рекомендуется):

```bash
pip install "apache-airflow==2.7.3" "psycopg2-binary==2.9.9"
```

### 2. Создание базы данных PostgreSQL

```sql
CREATE DATABASE tutor_platform;
```

Убедитесь, что в файле DAG корректны параметры подключения:

```python
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "tutor_platform",
    "user":     "postgres",
    "password": "postgres",
}
```

### 3. Инициализация Airflow

```bash
# Инициализация метабазы Airflow
airflow db init

# Создание пользователя для веб-интерфейса
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email admin@example.com
```

### 4. Копирование DAG

```bash
# По умолчанию DAG-файлы хранятся в ~/airflow/dags/
cp tutor_platform_dag.py ~/airflow/dags/
```

Проверить путь к папке DAGs можно командой:

```bash
airflow config get-value core dags_folder
```

### 5. Запуск Airflow

Открыть **два отдельных терминала**:

```bash
# Терминал 1 — веб-сервер
airflow webserver --port 8080

# Терминал 2 — планировщик
airflow scheduler
```

### 6. Запуск DAG

Открыть браузер: [http://localhost:8080](http://localhost:8080)

1. Войти под admin / admin
2. Найти DAG `tutor_platform_load`
3. Включить DAG (переключатель слева)
4. Нажать кнопку **Trigger DAG** (▶)
5. Перейти в **Graph View** для наблюдения за выполнением задач

Либо запустить через CLI:

```bash
airflow dags trigger tutor_platform_load
```

---
### 6. Результат работы
![Screenshoot](/screenshot_1.png)
![Screenshoot](/screenshot_2.png)

## Проверка результатов

После успешного выполнения DAG проверить данные в PostgreSQL:

```sql
-- Общая статистика
SELECT 'Subject'  AS tbl, COUNT(*) FROM Subject
UNION ALL
SELECT 'Tutor',          COUNT(*) FROM Tutor
UNION ALL
SELECT 'Student',        COUNT(*) FROM Student
UNION ALL
SELECT 'Lesson',         COUNT(*) FROM Lesson
UNION ALL
SELECT 'Payment',        COUNT(*) FROM Payment
UNION ALL
SELECT 'Review',         COUNT(*) FROM Review;

-- Средний рейтинг репетиторов
SELECT t.name, ROUND(AVG(r.rating), 2) AS avg_rating, COUNT(r.id) AS reviews
FROM Tutor t
JOIN Lesson l  ON l.tutor_id = t.id
JOIN Review r  ON r.lesson_id = l.id
GROUP BY t.name
ORDER BY avg_rating DESC;

-- Выручка по предметам
SELECT s.name AS subject, SUM(p.amount) AS total_revenue
FROM Subject s
JOIN Lesson l  ON l.subject_id = s.id
JOIN Payment p ON p.lesson_id = l.id
GROUP BY s.name
ORDER BY total_revenue DESC;
```

---

## Структура файлов

```
lab2/
├── tutor_platform_dag.py   # DAG с генерацией тестовых данных
└── README.md               # Данный файл
```

---

## Возможные ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `connection refused` | PostgreSQL не запущен | `sudo service postgresql start` |
| `database "tutor_platform" does not exist` | БД не создана | `CREATE DATABASE tutor_platform;` |
| `ModuleNotFoundError: psycopg2` | Не установлен драйвер | `pip install psycopg2-binary` |
| DAG не появляется в UI | Файл не в папке dags | Проверить путь командой выше |
| `AirflowNotFoundException` | Scheduler не запущен | Запустить `airflow scheduler` |
