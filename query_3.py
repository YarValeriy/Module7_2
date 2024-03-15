from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Base, Group, Student, Teacher, Subject, Grade

engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Session = sessionmaker(bind=engine)
session = Session()

# SELECT g.name, sub.name, AVG(gr.grade) AS average_grade
# FROM grades gr
# JOIN students s ON gr.student_id = s.id
# JOIN groups g ON s.group_id = g.id
# JOIN subjects sub ON gr.subject_id = sub.id
# WHERE sub.id = 3
# GROUP BY g.name, sub.name;


# Define the query
query = (
    session.query(
        Group.name.label("group_name"),
        Subject.name.label("subject_name"),
        func.avg(Grade.grade).label("average_grade"),
    )
    .join(Student, Grade.student_id == Student.id)
    .join(Group, Student.group_id == Group.id)
    .join(Subject, Grade.subject_id == Subject.id)
    .filter(Subject.id == 3)
    .group_by(Group.name, Subject.name)
)

# Execute the query and fetch all results
results = query.all()

# Print the results
for result in results:
    print(result.group_name, result.subject_name, result.average_grade)
