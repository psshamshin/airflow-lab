from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import psycopg2

# ─── Конфигурация подключения к БД ───────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "tutor_platform",
    "user": "postgres",
    "password": "postgres",
}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── Вспомогательная функция подключения ─────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ─── Шаг 1: создание таблиц ──────────────────────────────────────────────────
def create_tables():
    ddl = """
    CREATE TABLE IF NOT EXISTS Subject (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL UNIQUE,
        category VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS Tutor (
        id SERIAL PRIMARY KEY,
        name VARCHAR(150) NOT NULL,
        email VARCHAR(150) NOT NULL UNIQUE,
        bio TEXT,
        hourly_rate NUMERIC(8,2)
    );

    CREATE TABLE IF NOT EXISTS TutorSubject (
        tutor_id INT REFERENCES Tutor(id) ON DELETE CASCADE,
        subject_id INT REFERENCES Subject(id) ON DELETE CASCADE,
        experience_years INT,
        PRIMARY KEY (tutor_id, subject_id)
    );

    CREATE TABLE IF NOT EXISTS Student (
        id SERIAL PRIMARY KEY,
        name VARCHAR(150) NOT NULL,
        email VARCHAR(150) NOT NULL UNIQUE,
        birth_date DATE
    );

    CREATE TABLE IF NOT EXISTS Lesson (
        id SERIAL PRIMARY KEY,
        tutor_id INT REFERENCES Tutor(id),
        student_id INT REFERENCES Student(id),
        subject_id INT REFERENCES Subject(id),
        scheduled_at TIMESTAMP NOT NULL,
        duration_min INT DEFAULT 60,
        status VARCHAR(20) DEFAULT 'scheduled'
    );

    CREATE TABLE IF NOT EXISTS Payment (
        id SERIAL PRIMARY KEY,
        lesson_id INT REFERENCES Lesson(id) UNIQUE,
        amount NUMERIC(10,2),
        method VARCHAR(50),
        paid_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS Review (
        id SERIAL PRIMARY KEY,
        lesson_id INT REFERENCES Lesson(id) UNIQUE,
        rating INT CHECK (rating BETWEEN 1 AND 5),
        comment TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    conn.close()
    print("Таблицы успешно созданы.")


# ─── Шаг 2: генерация и загрузка предметов ───────────────────────────────────
def load_subjects():
    subjects = [
        ("Математика",      "Точные науки"),
        ("Физика",          "Точные науки"),
        ("Химия",           "Точные науки"),
        ("Информатика",     "Точные науки"),
        ("Русский язык",    "Гуманитарные"),
        ("Английский язык", "Иностранные языки"),
        ("История",         "Гуманитарные"),
        ("Биология",        "Естественные науки"),
        ("Литература",      "Гуманитарные"),
        ("Обществознание",  "Гуманитарные"),
    ]
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO Subject (name, category) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                subjects,
            )
    conn.close()
    print(f"Загружено {len(subjects)} предметов.")


# ─── Шаг 3: генерация репетиторов ────────────────────────────────────────────
def load_tutors():
    first_names = ["Иван", "Мария", "Алексей", "Ольга", "Дмитрий",
                   "Анна", "Сергей", "Елена", "Павел", "Наталья"]
    last_names  = ["Иванов", "Петрова", "Сидоров", "Кузнецова", "Морозов",
                   "Новикова", "Попов", "Волкова", "Козлов", "Соколова"]

    tutors = []
    for i in range(20):
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        name  = f"{fn} {ln}"
        email = f"tutor{i+1}@example.com"
        bio   = f"Репетитор с опытом работы {random.randint(1, 15)} лет."
        rate  = round(random.uniform(800, 3000), 2)
        tutors.append((name, email, bio, rate))

    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO Tutor (name, email, bio, hourly_rate)
                   VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                tutors,
            )
    conn.close()
    print(f"Загружено {len(tutors)} репетиторов.")


# ─── Шаг 4: привязка репетиторов к предметам ─────────────────────────────────
def load_tutor_subjects():
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM Tutor")
            tutor_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT id FROM Subject")
            subject_ids = [r[0] for r in cur.fetchall()]

            records = []
            for tid in tutor_ids:
                chosen = random.sample(subject_ids, k=random.randint(1, 3))
                for sid in chosen:
                    records.append((tid, sid, random.randint(1, 10)))

            cur.executemany(
                """INSERT INTO TutorSubject (tutor_id, subject_id, experience_years)
                   VALUES (%s, %s, %s) ON CONFLICT DO NOTHING""",
                records,
            )
    conn.close()
    print(f"Загружено {len(records)} связей репетитор–предмет.")


# ─── Шаг 5: генерация учеников ───────────────────────────────────────────────
def load_students():
    first_names = ["Андрей", "Виктория", "Максим", "Юлия", "Роман",
                   "Кристина", "Артём", "Светлана", "Никита", "Валерия"]
    last_names  = ["Белов", "Фёдорова", "Громов", "Тихонова", "Лебедев",
                   "Никитина", "Орлов", "Зайцева", "Борисов", "Яковлева"]

    students = []
    for i in range(50):
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        name  = f"{fn} {ln}"
        email = f"student{i+1}@example.com"
        year  = random.randint(2000, 2010)
        month = random.randint(1, 12)
        day   = random.randint(1, 28)
        birth = f"{year}-{month:02d}-{day:02d}"
        students.append((name, email, birth))

    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO Student (name, email, birth_date)
                   VALUES (%s, %s, %s) ON CONFLICT DO NOTHING""",
                students,
            )
    conn.close()
    print(f"Загружено {len(students)} учеников.")


# ─── Шаг 6: генерация занятий ────────────────────────────────────────────────
def load_lessons():
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM Tutor")
            tutor_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT id FROM Student")
            student_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT id FROM Subject")
            subject_ids = [r[0] for r in cur.fetchall()]

            statuses = ["completed", "completed", "completed", "cancelled", "scheduled"]
            lessons  = []
            base_dt  = datetime(2024, 1, 1)

            for _ in range(200):
                tid    = random.choice(tutor_ids)
                sid    = random.choice(student_ids)
                sub_id = random.choice(subject_ids)
                offset = timedelta(days=random.randint(0, 365),
                                   hours=random.randint(8, 20))
                sched  = base_dt + offset
                dur    = random.choice([45, 60, 90])
                status = random.choice(statuses)
                lessons.append((tid, sid, sub_id, sched, dur, status))

            cur.executemany(
                """INSERT INTO Lesson
                       (tutor_id, student_id, subject_id, scheduled_at, duration_min, status)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                lessons,
            )
    conn.close()
    print(f"Загружено {len(lessons)} занятий.")


# ─── Шаг 7: генерация оплат ──────────────────────────────────────────────────
def load_payments():
    methods = ["card", "cash", "transfer"]
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT l.id, t.hourly_rate, l.duration_min, l.scheduled_at
                FROM Lesson l
                JOIN Tutor t ON l.tutor_id = t.id
                WHERE l.status = 'completed'
            """)
            lessons = cur.fetchall()

            records = []
            for lesson_id, rate, duration, sched in lessons:
                amount   = round(float(rate) * duration / 60, 2)
                method   = random.choice(methods)
                paid_at  = sched + timedelta(hours=random.randint(0, 24))
                records.append((lesson_id, amount, method, paid_at))

            cur.executemany(
                """INSERT INTO Payment (lesson_id, amount, method, paid_at)
                   VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                records,
            )
    conn.close()
    print(f"Загружено {len(records)} платежей.")


# ─── Шаг 8: генерация отзывов ────────────────────────────────────────────────
def load_reviews():
    comments = [
        "Отличный репетитор, всё объяснил понятно!",
        "Занятие прошло хорошо, буду продолжать.",
        "Немного сложно, но полезно.",
        "Репетитор профессионал своего дела.",
        "Хорошая подача материала, рекомендую.",
        "Занятие понравилось, результат есть.",
        "Всё чётко и по делу.",
        "Очень доволен, оценки улучшились.",
    ]
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT l.id, l.scheduled_at
                FROM Lesson l
                WHERE l.status = 'completed'
            """)
            lessons = cur.fetchall()

            records = []
            for lesson_id, sched in lessons:
                if random.random() < 0.75:  # 75% занятий получают отзыв
                    rating     = random.randint(3, 5)
                    comment    = random.choice(comments)
                    created_at = sched + timedelta(hours=random.randint(1, 48))
                    records.append((lesson_id, rating, comment, created_at))

            cur.executemany(
                """INSERT INTO Review (lesson_id, rating, comment, created_at)
                   VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                records,
            )
    conn.close()
    print(f"Загружено {len(records)} отзывов.")


# ─── Определение DAG ─────────────────────────────────────────────────────────
with DAG(
    dag_id="tutor_platform_load",
    default_args=default_args,
    description="Генерация и загрузка тестовых данных репетиторской платформы",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lab2", "tutor_platform"],
) as dag:

    t_create   = PythonOperator(task_id="create_tables",     python_callable=create_tables)
    t_subjects = PythonOperator(task_id="load_subjects",     python_callable=load_subjects)
    t_tutors   = PythonOperator(task_id="load_tutors",       python_callable=load_tutors)
    t_ts       = PythonOperator(task_id="load_tutor_subjects", python_callable=load_tutor_subjects)
    t_students = PythonOperator(task_id="load_students",     python_callable=load_students)
    t_lessons  = PythonOperator(task_id="load_lessons",      python_callable=load_lessons)
    t_payments = PythonOperator(task_id="load_payments",     python_callable=load_payments)
    t_reviews  = PythonOperator(task_id="load_reviews",      python_callable=load_reviews)

    # Порядок выполнения
    t_create >> [t_subjects, t_tutors, t_students]
    t_tutors >> t_ts
    [t_ts, t_students, t_subjects] >> t_lessons
    t_lessons >> t_payments
    t_lessons >> t_reviews
