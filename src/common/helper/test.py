import pandas as pd

# Sample DataFrame
data = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'David'],
    'age': [25, 17, 40, 30],  # Bob is underage
    'salary': [50000, 60000, -1000, 0],  # Charlie has negative salary, David has zero salary
    'email': ['alice@example.com', 'bobexample.com', 'charlie@example.com', 'david@example.com']  # Bob has an invalid email
})

# Validation functions
validation_functions = {
    'age': lambda x: x >= 18,  # Age should be at least 18
    'salary': lambda x: x > 0,  # Salary should be positive
    'email': lambda x: '@' in x  # Email should contain '@'
}

# Apply validations to each column and create a validation report
report_data = {f'validate_{name}': data[name].apply(func) for name, func in validation_functions.items()}
report_df = pd.DataFrame(report_data)

# Check if all validations are passed (True for valid, False for invalid)
report_df['all_valid'] = report_df.all(axis=1)

# Separate valid and invalid data
valid_data_df = data[report_df['all_valid']]  # Rows where all validations passed
invalid_data_df = data[~report_df['all_valid']]  # Rows where at least one validation failed

# Display results
print("Validation Report:")
print(report_df)
print("\nValid Data:")
print(valid_data_df)
print("\nInvalid Data:")
print(invalid_data_df)
