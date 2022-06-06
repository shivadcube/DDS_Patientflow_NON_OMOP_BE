import os
import sys
from datetime import datetime, timedelta
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from . mailer import AWSMailSES

from . mail_constants import email_view_project

template_path = os.path.expanduser(os.getcwd()+"/../templates/")


class MailTemplate:
    def send_template(self, status="FAILED", to_address=[], data={}):
        username = data.get('username', '')
        project_id = str(data.get('project_id', ''))
        project_name = data.get('project_name', '')
        analysis_name = data.get('analysis_name', '')
        created_at = data.get('created_at', '')
        modified_at = data.get('modified_at', '')
        attribute_type = data.get('attribute_type', '')
        attribute_type_name = "Line of Therapy" if attribute_type == 'lot' else attribute_type.capitalize()
        status_type = "{} Status".format(attribute_type.capitalize())
        completion_time = self.to_find_time_difference(str(created_at), str(modified_at))
        error_message = data.get('error_message')
        view_project = email_view_project
        view_project = self.check_url(view_project)
        view_project = view_project+project_id

        if status.upper() == 'FINISHED':
            subject = "DDS PF Suite: " + project_id + "(" + project_name + ") " + "- " + status_type + ": Completed"
            html = open(template_path+'success_template.html').read()
            html = html.replace("{User Name}", username)
            html = html.replace("{Project Name}", project_name)
            html = html.replace("{Project ID}", project_id)
            html = html.replace("{Analysis Name}", analysis_name)
            html = html.replace("{Attribute Type}", attribute_type_name)
            html = html.replace("{Status}", "Completed")
            html = html.replace("{Completion Time}", completion_time)
            body = html.replace("{view_project}", view_project)
            mail = AWSMailSES()
            mail.send_mail(to_address=to_address, subject=subject, body=body)

        elif status:
            subject = "DDS PF Suite: " + project_id + "(" + project_name + ") " + "- " + status_type + ": Failed"
            html = open(template_path + 'failed_template.html').read()
            html = html.replace("{User Name}", username)
            html = html.replace("{Project Name}", project_name)
            html = html.replace("{Project ID}", project_id)
            html = html.replace("{Analysis Name}", analysis_name)
            html = html.replace("{Attribute Type}", attribute_type_name)
            html = html.replace("{Status}", "Failed")
            html = html.replace("{Reason}", error_message)
            body = html.replace("{view_project}", view_project)
            mail = AWSMailSES()
            mail.send_mail(to_address=to_address, subject=subject, body=body)

    def check_url(self, url):
        if url.endswith("/"):
            return url
        return url+"/"

    def to_find_time_difference(self, created_at, modified_at):
        created_at = created_at[:len(created_at) - 7]
        modified_at = modified_at[:len(modified_at) - 7]
        d1 = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
        d2 = datetime.strptime(modified_at, '%Y-%m-%d %H:%M:%S')
        diff_in_sec = (d2 - d1).total_seconds()
        diff_in_min = str(timedelta(seconds=diff_in_sec))
        h, m, s = diff_in_min.split(':')
        return "{} Min {} Sec".format(int(h) * 60 + int(m), int(s))
