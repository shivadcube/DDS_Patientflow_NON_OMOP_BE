from django.http import HttpResponse

import datetime
import environ
env = environ.Env()
environ.Env.read_env()


class AuthenticationMiddleware(object):

    def check_session_expiry(self, request):
        try:
            # get current time and session time
            current_time = datetime.datetime.now()
            session_time = request.session.get('session_time', 0)
            expiry_time = env('SESSION_EXPIRY')
            if session_time:
                # find diff between current time and session time
                session_time = datetime.datetime.fromisoformat(session_time)
                diff_time = current_time - session_time
                minutes = diff_time.seconds / 60
                if int(expiry_time) < minutes:
                    return False
                # assign current time to session time
                request.session['session_time'] = str(current_time)
                return True
            return False
        except Exception as e:
            print(e)
            return False

    def process_view(self, request, view_func, view_args, view_kwargs):
        # assign default values for auth and session
        request.is_authenticated = False
        request.session_expiry = False
        path = request.path
        if path.endswith('/'):
            path = path[0:-1]
        if path.endswith('login') or path.endswith('logout'):
            request.session['user_id'] = 0
            request.session['token'] = ''
            request.session['session_time'] = ''
            return
        elif path.find('cohort') >= 0 or path.find('download') >= 0:
            print("Cohort or Download API")
        elif not path.endswith('login'):
            # get auth token and user id
            actual_token = request.session.get('token', '')
            user_id = request.session.get('user_id', 0)
            if actual_token and user_id != 0:
                # get browser token
                browser_token = request.META.get('HTTP_AUTHORIZATION')
                if browser_token:
                    browser_token = browser_token.replace('Token ', '').strip()
                    request.session_expiry = self.check_session_expiry(request)
                    # check auth token and browser
                    if browser_token == actual_token:
                        request.is_authenticated = True
            if not request.is_authenticated or not request.session_expiry:
                request.session['user_id'] = 0
                request.session['token'] = ''
                request.session['session_time'] = ''
                return HttpResponse('Unauthorized', status=401)
            return
        else:
            request.session['user_id'] = 0
            request.session['token'] = ''
            request.session['session_time'] = ''
            return

    def process_exception(self, request, exception):
        request.is_authenticated = False
        request.session_expiry = False
        request.session['user_id'] = 0
        request.session['token'] = ''
        request.session['session_time'] = ''
        return None
