from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Base, Group, Student, Teacher, Subject, Grade

engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Session = sessionmaker(bind=engine)
session = Session()

# SQL:
# SELECT students.name
# FROM students
# JOIN groups ON students.group_id = groups.id
# WHERE groups.name = 'Group B';

# Define the query
query = (
    session.query(Student.name, Group.name)
    .join(Group, Student.group)
    .filter(Group.id == 2)
)

# Execute the query and fetch the result
results = query.all()

# Print the results
for student_name, group_name in results:
    print(student_name, "-", group_name)
