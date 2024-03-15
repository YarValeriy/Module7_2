from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Base, Group, Student, Teacher, Subject, Grade

engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Session = sessionmaker(bind=engine)
session = Session()
# SQL:
# SELECT 
#     s.id, 
#     s.name, 
#     ROUND(AVG(g.grade), 2) AS average_grade
# FROM students s
# JOIN grades g ON s.id = g.student_id
# GROUP BY s.id
# ORDER BY average_grade DESC
# LIMIT 5;

# Define the query
query = (
    session.query(
        Student.id,
        Student.name,
        func.round(func.avg(Grade.grade), 2).label("average_grade"),
    )
    .join(Grade, Student.id == Grade.student_id)
    .group_by(Student.id)
    .order_by(func.round(func.avg(Grade.grade), 2).desc())
    .limit(5)
)

# Execute the query and fetch the results
result = query.all()

# Print the results
for row in result:
    print(row.id, row.name, row.average_grade)
