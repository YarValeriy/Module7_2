from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Base, Group, Student, Teacher, Subject, Grade

engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Session = sessionmaker(bind=engine)
session = Session()

# SELECT s.name AS student_name, AVG(g.grade) AS gpa
# FROM students s
# JOIN grades g ON s.id = g.student_id
# JOIN subjects sub ON g.subject_id = sub.id
# WHERE sub.name = 'Mathematics'
# GROUP BY s.id
# ORDER BY AVG(g.grade) DESC
# LIMIT 1;


# Define the query
query = (
    session.query(
        Student.name.label("student_name"), func.avg(Grade.grade).label("gpa")
    )
    .join(Grade, Student.id == Grade.student_id)
    .join(Subject, Grade.subject_id == Subject.id)
    .filter(Subject.name == "Mathematics")
    .group_by(Student.id)
    .order_by(func.avg(Grade.grade).desc())
    .limit(1)
)

# Execute the query and fetch the result
result = query.first()

# Print the result
print(result.student_name, result.gpa)
