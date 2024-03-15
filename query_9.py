from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Base, Group, Student, Teacher, Subject, Grade

engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Session = sessionmaker(bind=engine)
session = Session()

# SQL:
# SELECT students.name, subjects.name
# FROM grades
# JOIN subjects ON grades.subject_id = subjects.id
# JOIN students ON grades.student_id = students.id
# WHERE students.id = 1
# GROUP BY students.name, subjects.name;
# Define the query
query = (
    session.query(Student.name, Subject.name)
    .join(Grade, Grade.student_id == Student.id)
    .join(Subject, Grade.subject_id == Subject.id)
    .filter(Student.id == 1)
    .group_by(Student.name, Subject.name)
)

# Execute the query and fetch the result
results = query.all()

# Print the results
for student_name, subject_name in results:
    print("Student:", student_name, "Subject:", subject_name)
