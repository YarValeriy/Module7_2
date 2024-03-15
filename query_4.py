from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import Base, Group, Student, Teacher, Subject, Grade

engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Session = sessionmaker(bind=engine)
session = Session()

# SQL:
# SELECT AVG(grade) AS average_score
# FROM grades;

# Define the query
query = session.query(func.avg(Grade.grade).label("average_score"))

# Execute the query and fetch the result
result = query.scalar()

# Print the result
print("Average Score:", result)
