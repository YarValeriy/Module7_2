from sqlalchemy import func, join
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Base, Group, Student, Teacher, Subject, Grade

engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Session = sessionmaker(bind=engine)
session = Session()

# SQL:
# SELECT groups.name, students.name, subjects.name, grades.grade
# FROM students
# JOIN grades ON students.id = grades.student_id
# JOIN subjects ON grades.subject_id = subjects.id
# JOIN groups ON students.group_id = groups.id
# WHERE groups.id = 2 AND subjects.id = 4;

# Define the query
query = (
    session.query(Group.name, Student.name, Subject.name, Grade.grade)
    .select_from(
        join(Group, Student, Group.id == Student.group_id)
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Grade.subject_id == Subject.id)
    )
    .filter(Group.id == 2, Subject.id == 4)
)

# Execute the query and fetch the result
results = query.all()

# Print the results
for group_name, student_name, subject_name, grade in results:
    print(
        group_name,
        student_name,
        subject_name,
        grade,
    )
