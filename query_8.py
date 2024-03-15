from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Base, Group, Student, Teacher, Subject, Grade

engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Session = sessionmaker(bind=engine)
session = Session()

# SQL:
# SELECT teachers.name, subjects.name, AVG(grades.grade) AS average_score
# FROM grades
# JOIN subjects ON grades.subject_id = subjects.id
# JOIN teachers ON subjects.teacher_id = teachers.id
# WHERE teachers.id = 2
# GROUP BY subjects.name, teachers.name;

from sqlalchemy import func

# Define the query
query = (
    session.query(
        Teacher.name, Subject.name, func.avg(Grade.grade).label("average_score")
    )
    .join(Subject, Subject.teacher_id == Teacher.id)
    .join(Grade, Grade.subject_id == Subject.id)
    .filter(Teacher.id == 2)
    .group_by(Subject.name, Teacher.name)
)

# Execute the query and fetch the result
results = query.all()

# Print the results
for teacher_name, subject_name, average_score in results:
    print(
        "Teacher:",
        teacher_name,
        "Subject:",
        subject_name,
        "Average Score:",
        average_score,
    )
