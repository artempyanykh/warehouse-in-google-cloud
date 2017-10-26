import string
import random
from datetime import timedelta
from datetime import datetime
from datetime import date
import pandas as pd

car_options = {
    'hyundai': ['solaris', 'elantra'],
    'kia': ['rio', 'ceed'],
    'bmw': ['series_1', 'series_2'],
    'nissan': ['micra', 'murano']
}


def reg_number_generator():
    part_1 = ''.join([random.choice(string.ascii_lowercase) for _ in range(2)])
    part_2 = ''.join([random.choice(string.digits) for _ in range(4)])
    return part_1 + part_2


def car_generator():
    make = random.choice(list(car_options.keys()))
    model = random.choice(car_options[make])
    reg_number = reg_number_generator()
    return {'make': make, 'model': model, 'reg_number': reg_number}


def fleet_generator(num):
    used_regs = {}
    generated_cars = []
    current_id = 1

    while current_id <= num:
        car = car_generator()
        reg_number = car['reg_number']
        if not (reg_number in used_regs):
            car['id'] = current_id
            generated_cars.append(car)
            current_id += 1

    return generated_cars


first_names = [
    'Gheorghe', 'Ioan', 'Vasile', 'Constantin', 'Ion', 'Alexandru', 'Nicolae', 'Mihai', 'Dumitru', 'Andrei',
    'Maria', 'Elena', 'Ana', 'Ioana', 'Mihaela', 'Andreea', 'Cristina', 'Mariana', 'Alexandra', 'Daniela'
]

last_names = ['Popescu', 'Radu', 'Dumitru', 'Stan', 'Stoica', 'Gheorghe', 'Matei', 'Ciobanu', 'Ionescu', 'Rusu']


def passport_no_generator():
    return ''.join([random.choice(string.digits) for _ in range(9)])


def date_generator(fro, till):
    delta = (till - fro).days
    increment = random.randint(0, delta)
    return fro + timedelta(days=increment)


birth_from_date = datetime.strptime('1981-01-01', '%Y-%m-%d').date()
birth_till_date = datetime.strptime('1997-12-12', '%Y-%m-%d').date()

latest_permit_date = datetime.strptime('1999-10-31', '%Y-%m-%d').date()


def user_generator():
    name = random.choice(first_names) + ' ' + random.choice(last_names)
    birth_date = date_generator(birth_from_date, birth_till_date)
    driving_permit_since = date_generator(birth_date, latest_permit_date)
    passport_no = passport_no_generator()
    return {
        'name': name,
        'passport_no': passport_no,
        'birth_date': birth_date,
        'driving_permit_since': driving_permit_since
    }


def user_base_generator(num):
    used_passports = {}
    generated_users = []
    current_id = 1

    while current_id <= num:
        user = user_generator()
        passport_no = user['passport_no']
        if not (passport_no in used_passports):
            user['id'] = current_id
            generated_users.append(user)
            current_id += 1

    return generated_users


def day_rents_generator(userbase, fleet):
    min_rents = 1
    max_rents = len(fleet)
    rented_num = random.randint(min_rents, max_rents)

    rented_cars = random.choices(fleet, k=rented_num)
    renters = random.choices(userbase, k=rented_num)

    return list(zip(renters, rented_cars))


def fine_generator(day: date, user, car):
    day_start = datetime.combine(day, datetime.min.time()) + timedelta(hours=6)
    day_end = datetime.combine(day, datetime.min.time()) + timedelta(hours=23)
    delta_in_mins = (day_end - day_start).seconds / 60

    registered_at = day_start + timedelta(minutes=random.randint(1, delta_in_mins))
    fine_amount = min([1000, max([int(random.expovariate(0.1)), 1])])

    return {
        'car_reg_number': car['reg_number'],
        'registered_at': registered_at,
        'fine_amount': fine_amount
    }


def to_rent_row(day, user, car):
    return {
        'user_id': user['id'],
        'car_id': car['id'],
        'rented_on': day
    }


business_start_date = datetime.strptime('2017-10-01', '%Y-%m-%d').date()
data_end_date = datetime.strptime('2017-10-31', '%Y-%m-%d').date()
days_to_generate = (data_end_date - business_start_date).days


def generate_data(num_users, num_cars, start_date, num_days, mean_fines_num):
    users = user_base_generator(num_users)
    fleet = fleet_generator(num_cars)
    rents = []
    fines = []

    for day in range(num_days):
        today_date = start_date + timedelta(days=day)

        day_rents = day_rents_generator(users, fleet)
        day_fines = []
        day_rent_rows = []
        for user, car in day_rents:
            fines_to_generate = int(random.expovariate(1.0 / mean_fines_num))
            new_fines = [fine_generator(today_date, user, car) for _ in range(fines_to_generate)]

            day_fines.extend(new_fines)
            day_rent_rows.append(to_rent_row(today_date, user, car))

        rents.extend(day_rent_rows)
        fines.extend(day_fines)

    for i, rent in enumerate(rents):
        rent['id'] = i + 1

    for i, fine in enumerate(fines):
        fine['id'] = i + 1

    return {
        'users': users,
        'cars': fleet,
        'rents': rents,
        'fines': fines
    }


if __name__ == '__main__':
    random.seed(0)
    data = generate_data(100, 50, business_start_date, 30, 1)

    users = pd.DataFrame.from_records(data['users'])
    cars = pd.DataFrame.from_records(data['cars'])
    rents = pd.DataFrame.from_records(data['rents'])
    fines = pd.DataFrame.from_records(data['fines'])

    users.to_csv('generated/users.csv', header=True, index=False, columns=['id', 'name', 'passport_no', 'birth_date', 'driving_permit_since'])
    cars.to_csv('generated/cars.csv', header=True, index=False, columns=['id', 'make', 'model', 'reg_number'])
    rents.to_csv('generated/rents.csv', header=True, index=False, columns=['id', 'user_id', 'car_id', 'rented_on'])
    fines.to_csv('generated/fines.csv', header=True, index=False, columns=['id', 'car_reg_number', 'registered_at', 'fine_amount'])

    print("Data has been generated.")
