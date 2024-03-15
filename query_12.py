from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Base, Group, Student, Teacher, Subject, Grade

engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Session = sessionmaker(bind=engine)
session = Session()

# SQL:
# SELECT groups.name, subjects.name, students.name, grades.grade, grades.date_of
# FROM grades
# JOIN subjects ON grades.subject_id = subjects.id
# JOIN students ON grades.student_id = students.id
# JOIN groups ON students.group_id = groups.id
# WHERE groups.id = 1
# AND subjects.id = 2
# AND grades.date_of =
# (
# SELECT MAX(date_of)
# FROM (
# SELECT grades.date_of
# FROM grades
# JOIN subjects ON grades.subject_id = subjects.id
# JOIN students ON grades.student_id = students.id
# JOIN groups ON students.group_id = groups.id
# WHERE groups.id = 1
# AND subjects.id = 2)
# )

# Define the subquery to select the maximum date_of value
subquery_max_date = (
    session.query(func.max(Grade.date_of).label("max_date"))
    .join(Subject, Grade.subject_id == Subject.id)
    .join(Student, Grade.student_id == Student.id)
    .join(Group, Student.group_id == Group.id)
    .filter(Group.id == 1, Subject.id == 2)
    .subquery()
)

# Define the main query to fetch the required data
query = (
    session.query(Group.name, Subject.name, Student.name, Grade.grade, Grade.date_of)
    .join(Student, Group.id == Student.group_id)
    .join(Grade, Student.id == Grade.student_id)
    .join(Subject, Grade.subject_id == Subject.id)
    .join(subquery_max_date, subquery_max_date.c.max_date == Grade.date_of)
    .filter(Group.id == 1, Subject.id == 2)
)

# Execute the query and fetch the results
results = query.all()

# Print the results
for result in results:
    print(result)
