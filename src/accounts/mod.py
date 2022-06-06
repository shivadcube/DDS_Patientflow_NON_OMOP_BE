from django.contrib.auth.models import User

def insert_user_id(user_id, email, username):
    try:
        user_id = User.objects.get(pk=int(user_id))
        if not user_id:
            user = User()
            user.id = int(user_id)
            user.email = email
            user.username = username
            user.save()
    except User.DoesNotExist:
        user = User()
        user.id = int(user_id)
        user.email = email
        user.username = username
        user.save()
    except Exception as e:
        return {'error': str(e)}

