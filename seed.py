from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from faker import Faker
from datetime import datetime, timedelta
import random

from models import Base, Group, Student, Teacher, Subject, Grade

engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Base.metadata.create_all(engine)
fake = Faker("uk-UA")
Session = sessionmaker(bind=engine)
session = Session()

# Generate groups
groups = ["Group A", "Group B", "Group C"]
for group_name in groups:
    group = Group(name=group_name)
    session.add(group)
session.commit()


## Generate teachers
teachers = [Teacher(name=fake.name()) for _ in range(5)]
session.add_all(teachers)
session.commit()


# Generate subjects and assign random teachers
subjects = [
    "Mathematics",
    "Physics",
    "Biology",
    "English",
    "Ukrainian",
    "History",
    "Geography",
]
for subject_name in subjects:
    teacher = random.choice(teachers)
    subject = Subject(name=subject_name, teacher=teacher)
    session.add(subject)
session.commit()


for _ in range(30):
    group = (
        session.query(Group).order_by(func.random()).first())  # Randomly select a group
    student = Student(name=fake.name(), group=group)
    session.add(student)
session.commit()


# Generate grades for each student in all subjects
for student in session.query(Student):
    for subject in session.query(Subject):
        for _ in range(random.randint(1, 20)):
            grade = random.randint(2, 5)
            date = datetime.now() - timedelta(days=random.randint(1, 365))
            grade = Grade(
                grade=grade, date_of=date, student=student, discipline=subject
            )
            session.add(grade)
session.commit()

# Close the session
session.close()

print("Database populated successfully.")
