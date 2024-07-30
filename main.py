import pandas as pd
from datetime import datetime


# Read Data
def read_data():
    course_data = pd.read_csv(f'course.csv', sep=',')
    course_attendance_data = pd.read_csv(f'course_attendance.csv', sep=',')
    enrollment_data = pd.read_csv(f'enrollment.csv', sep=',')
    schedule_data = pd.read_csv(f'schedule.csv', sep=',')
    return course_data, course_attendance_data, enrollment_data, schedule_data

# Function to generate course dates for each row
def get_course_dates(start_date, end_date, course_days):
    all_dates = pd.date_range(start=start_date, end=end_date)
    
    return [date for date in all_dates if ((date.isoweekday() % 7) + 1) in course_days]

def get_week_of_month(dates_list):
    week_of_month = []
    
    for date_str in dates_list:
        # Convert string to datetime if needed
        if isinstance(date_str, str):
            date = datetime.strptime(date_str, '%Y-%m-%d')
        else:
            date = date_str
        
        day_of_month = date.day
        # Calculate the week of the month
        week_of_month_num = (day_of_month - 1) // 7 + 1
        
        # Get the month name
        month_name = date.strftime('%B')  # Full month name (e.g., 'September')
        
        # Combine week number and month name
        combined = f"W{week_of_month_num}-{month_name}"
        week_of_month.append(combined)
    
    return week_of_month

def schedule_data_details(data):
    # Schedule Data Details
    data['START_DT'] = pd.to_datetime(data['START_DT'], format="%d-%b-%y")
    data['END_DT'] = pd.to_datetime(data['END_DT'], format="%d-%b-%y")
    data['COURSE_DAYS'] = data['COURSE_DAYS'].apply(lambda x: list(map(int, x.split(','))))

    # Apply the function get_course_dates
    data['COURSE_DATES'] = data.apply(lambda row: get_course_dates(row['START_DT'], row['END_DT'], row['COURSE_DAYS']), axis=1)
    schedule_details_data = pd.DataFrame(data[['ID', 'COURSE_ID', 'COURSE_DATES']])
    schedule_details_data = schedule_details_data.rename(columns={'ID': 'SCHEDULE_ID'})
    schedule_details_data = schedule_details_data.explode('COURSE_DATES')
    schedule_details_data['ID'] = schedule_details_data.reset_index().index + 1
    schedule_details_data = schedule_details_data[['ID', 'SCHEDULE_ID', 'COURSE_ID', 'COURSE_DATES']]
    schedule_details_data['WEEK_OF_MONTH'] = get_week_of_month(schedule_details_data['COURSE_DATES'])
    count_all_schedules = schedule_details_data.groupby(['SCHEDULE_ID', 'COURSE_ID', 'WEEK_OF_MONTH']).size().reset_index(name='total_course_in_week')
    return count_all_schedules

def main():
    course_data, course_attendance_data, enrollment_data, schedule_data = read_data()
    count_all_schedules = schedule_data_details(schedule_data)

    
    # course_attendance_data['ATTEND_DT'] = course_attendance_data.ATTEND_DT.dt.strftime('%Y-%m-%d')

    #Get Data Join
    data_join_semester = pd.merge(course_attendance_data, enrollment_data, on=['STUDENT_ID', 'SCHEDULE_ID'], how='left')
    data_join_semester = data_join_semester[['STUDENT_ID', 'SCHEDULE_ID', 'ATTEND_DT', 'SEMESTER']]

    data_join_semester['ATTEND_DT'] = pd.to_datetime(data_join_semester['ATTEND_DT'], format="%d-%b-%y")

    data_join_semester['WEEK_OF_MONTH'] = get_week_of_month(data_join_semester['ATTEND_DT'])
    count_attendance_student = data_join_semester.groupby(['STUDENT_ID', 'SCHEDULE_ID', 'WEEK_OF_MONTH', 'SEMESTER']).size().reset_index(name='total_attendance_in_week')
    merge_data = pd.merge(count_attendance_student, count_all_schedules, on=['SCHEDULE_ID', 'WEEK_OF_MONTH'], how='left')
    get_total_course = merge_data[['STUDENT_ID', 'SCHEDULE_ID', 'WEEK_OF_MONTH', 'total_attendance_in_week', 'total_course_in_week', 'SEMESTER']]
    # get_total_course['ATTENDANCE_PCT'] = round((get_total_course['total_attendance_in_week']/get_total_course['total_course_in_week'])*100, 2)

    # course_name
    schedule_data = schedule_data.rename(columns={'ID': 'SCHEDULE_ID'})
    merge_data_course_name = pd.merge(get_total_course, schedule_data, on=['SCHEDULE_ID'], how='left')
    merge_data_course_name = merge_data_course_name[['STUDENT_ID', 'SCHEDULE_ID', 'WEEK_OF_MONTH', 'total_attendance_in_week', 'total_course_in_week', 'SEMESTER', 'COURSE_ID']]
    
    course_data = course_data.rename(columns={'ID': 'COURSE_ID'})
    merge_data_course_name = pd.merge(merge_data_course_name, course_data, on=['COURSE_ID'], how='left')
    merge_data_course_name = merge_data_course_name[['STUDENT_ID', 'SCHEDULE_ID', 'WEEK_OF_MONTH', 'total_attendance_in_week', 'total_course_in_week', 'SEMESTER', 'NAME']]

    # Calculate
    
    merge_data_course_name = merge_data_course_name[['SEMESTER', 'WEEK_OF_MONTH', 'NAME', 'total_attendance_in_week', 'total_course_in_week']]
    grouped_data = merge_data_course_name.groupby(['SEMESTER', 'WEEK_OF_MONTH', 'NAME']).agg({
    'total_attendance_in_week': 'sum',
    'total_course_in_week': 'sum'
}).reset_index()
    grouped_data['ATTENDANCE_PCT'] = round((grouped_data['total_attendance_in_week']/grouped_data['total_course_in_week'])*100, 2)
    final_data = grouped_data.rename(columns={'SEMESTER': 'SEMESTER_ID', 'WEEK_OF_MONTH':'WEEK_ID', 'NAME':'COURSE_NAME', 'total_attendance_in_week':'total_attendance', 'total_course_in_week':'total_course'})
    final_data.to_csv(f'result.csv', sep=';', index=False)
    return 'Success'

a = main()
print(a)
