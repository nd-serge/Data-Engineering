import dlt 


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
						#destination='duckdb', 
						dataset_name='customer')
# run the pipeline with default settings, and capture the outcome
info = pipeline.run(customers_2, 
                    table_name="shoppy_users", 
                    write_disposition="merge",
                    primary_key="ID")
  