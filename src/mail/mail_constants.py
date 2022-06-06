import os
import sys
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

project_folder = os.path.expanduser(os.getcwd() + "/../settings")
load_dotenv(os.path.join(project_folder, '.env'))

email_region_name = os.getenv("email_region_name", "user")

email_role_arn = os.getenv("email_role_arn", "user")

email_sender = os.getenv("email_sender", "user")

email_source_arn = os.getenv("email_source_arn", "user")

email_view_project= os.getenv("email_view_project", "")
