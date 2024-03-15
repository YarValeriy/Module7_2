from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Base, Group, Student, Teacher, Subject, Grade

engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Session = sessionmaker(bind=engine)
session = Session()

# SQL:
# SELECT teachers.name AS teacher, subjects.name AS subject, students.name AS student
# FROM grades
# JOIN subjects ON grades.subject_id = subjects.id
# JOIN teachers ON subjects.teacher_id = teachers.id
# JOIN students ON grades.student_id = students.id
# WHERE students.id = 1
# AND teachers.id = 2
# GROUP BY students.name, subjects.name, teachers.name;
# Define the query
query = (
    session.query(Teacher.name, Subject.name, Student.name)
    .join(Subject, Subject.teacher_id == Teacher.id)
    .join(Grade, Grade.subject_id == Subject.id)
    .join(Student, Grade.student_id == Student.id)
    .filter(Student.id == 1, Teacher.id == 2)
    .group_by(Student.name, Subject.name, Teacher.name)
)

# Execute the query and fetch the result
results = query.all()

# Print the results
for teacher_name, subject_name, student_name in results:
    print("Teacher:", teacher_name, "Subject:", subject_name, "Student:", student_name)

