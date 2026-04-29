"""
Add or update a Lyve user account.
Usage: python3.11 add_user.py <email> <password>
Example: python3.11 add_user.py matt@lyvestudio.com mypassword123
"""
import sys
import json
import os
from passlib.hash import bcrypt

USERS_FILE = "users.json"
ALLOWED_DOMAIN = "lyvestudio.com"

if len(sys.argv) != 3:
    print("Usage: python3.11 add_user.py <email> <password>")
    sys.exit(1)

email = sys.argv[1].strip().lower()
password = sys.argv[2]

if not email.endswith(f"@{ALLOWED_DOMAIN}"):
    print(f"Error: email must end with @{ALLOWED_DOMAIN}")
    sys.exit(1)

users = {}
if os.path.exists(USERS_FILE):
    with open(USERS_FILE) as f:
        users = json.load(f)

users[email] = bcrypt.hash(password)

with open(USERS_FILE, "w") as f:
    json.dump(users, f, indent=2)

print(f"✓ User '{email}' saved to {USERS_FILE}")
