from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Base, Group, Student, Teacher, Subject, Grade

engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Session = sessionmaker(bind=engine)
session = Session()

# SQL:
# SELECT subjects.name, teachers.name
# FROM subjects
# JOIN teachers ON subjects.teacher_id = teachers.id
# WHERE teachers.id = 2;
from sqlalchemy.orm import aliased

# Define aliases for the tables
Subject = aliased(Subject)
Teacher = aliased(Teacher)

# Define the query
query = (
    session.query(Subject.name, Teacher.name)
    .join(Teacher, Subject.teacher)
    .filter(Teacher.id == 2)
)

# Execute the query and fetch the result
results = query.all()

# Print the results
for subject_name, teacher_name in results:
    print("Subject:", subject_name, "Teacher:", teacher_name)
