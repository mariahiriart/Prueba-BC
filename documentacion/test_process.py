import json
from silent_listener_v1_1 import process_message

# Load test payload
with open("test_payload.json", "r") as f:
    payload = json.load(f)

# Run process_message
print("--- TEST DIRECTO DE PROCESAMIENTO ---")
process_message(payload)
print("--- TEST COMPLETADO ---")
