import json

# Path to the JSON file
file_path = '/home/hadoop/cs424_project/hdfs/spotify_data/recommendation_data.json'

# Fields to remove
fields_to_remove = ['key', 'timestamp', 'topic', 'offset', 'partition']

# Read the JSON data
with open(file_path, 'r') as file:
    data = []
    for line in file:
        entry = json.loads(line)
        # Parse the 'value' field if it exists and is a string
        if 'value' in entry and isinstance(entry['value'], str):
            entry['value'] = json.loads(entry['value'])
        data.append(entry)

# Remove the specified fields
for entry in data:
    for field in fields_to_remove:
        if field in entry:
            del entry[field]

# Save the cleaned data back to the file
with open(file_path, 'w') as file:
    for entry in data:
        # Convert the 'value' field back to a JSON string if it exists
        if 'value' in entry and isinstance(entry['value'], dict):
            entry['value'] = json.dumps(entry['value'])
        file.write(json.dumps(entry) + '\n')