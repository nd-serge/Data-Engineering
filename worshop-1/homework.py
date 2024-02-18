import dlt 

# Question 1
'''
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 5
generator = square_root_generator(limit)
sum = 0

for sqrt_value in generator:
    sum += sqrt_value

print('the sum is ', sum)
'''


# Question 2 
'''
generator = square_root_generator(13)

for index, value in enumerate(range(0, 13)):
    print(index, next(generator))
'''


# ingest first data
# def people_1():
#     for i in range(1, 6):
#         yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

# generator_1 = people_1()

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

generator_2 = people_2()
customers_2 = list(generator_2)






pipeline = dlt.pipeline(pipeline_name="customer_data",
						destination='duckdb', 
						dataset_name='customer')
# run the pipeline with default settings, and capture the outcome
info = pipeline.run(customers_2, 
                    table_name="shoppy_users", 
                    write_disposition="append")
                    #primary_key="ID")
  