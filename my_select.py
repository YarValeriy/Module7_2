from sqlalchemy import func, join
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy import create_engine
from models import Base, Group, Student, Teacher, Subject, Grade


engine = create_engine("postgresql://postgres:567234@localhost/Module7")
Session = sessionmaker(bind=engine)
session = Session()
Subject = aliased(Subject)
Teacher = aliased(Teacher)

# Define the query

def select_1():
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


def select_2():
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

def select_3():
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

def select_4():
    # Define the query
    query = session.query(func.avg(Grade.grade).label("average_score"))

    # Execute the query and fetch the result
    result = query.scalar()

    # Print the result
    print("Average Score:", result)

def select_5():
# Define aliases for the tables
    # Subject = aliased(Subject)
    # Teacher = aliased(Teacher)

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

def select_6():
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

def select_7():
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

def select_8():
    query = (
        session.query(
            Teacher.name, Subject.name, func.avg(Grade.grade).label("average_score")
        )
        .join(Subject, Subject.teacher_id == Teacher.id)
        .join(Grade, Grade.subject_id == Subject.id)
        .filter(Teacher.id == 2)
        .group_by(Subject.name, Teacher.name)
    )

    # Execute the query and fetch the result
    results = query.all()

    # Print the results
    for teacher_name, subject_name, average_score in results:
        print(
            "Teacher:",
            teacher_name,
            "Subject:",
            subject_name,
            "Average Score:",
            average_score,
        )

def select_9():
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


def select_10():
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


def select_11():
    query = (
        session.query(
            Teacher.name.label("teacher"),
            func.avg(Grade.grade).label("average_grade"),
            Student.name.label("student"),
        )
        .join(Subject, Subject.teacher_id == Teacher.id)
        .join(Grade, Grade.subject_id == Subject.id)
        .join(Student, Grade.student_id == Student.id)
        .filter(Student.id == 1, Teacher.id == 2)
        .group_by(Teacher.name, Student.name)
    )

    # Execute the query and fetch the result
    results = query.all()    
    # Print the results
    for teacher_name, average_grade, student_name in results:
        print(
            "Teacher:",
            teacher_name,
            "Average Grade:",
            average_grade,
            "Student:",
            student_name,
        )

def select_12():
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

def main():
    commands = list()  # list of command objects
    commands.append(select_1)
    commands.append(select_2)
    commands.append(select_3)
    commands.append(select_4)
    commands.append(select_5)
    commands.append(select_6)
    commands.append(select_7)
    commands.append(select_8)
    commands.append(select_9)
    commands.append(select_10)
    commands.append(select_11)
    commands.append(select_12)

    while True:
        user_input = input("Enter request number for selection or another key to exit>")
        if user_input.isnumeric():
            if int(user_input) in range(1,13):
                sel = commands[int(user_input)-1]
                sel()
                continue
            else:
                break
        else:
            break
    return


if __name__ == "__main__":
    main()
