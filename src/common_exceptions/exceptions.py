from DDS3_ADMIN import api_constants


class CommonBaseException(Exception):

    def __init__(self, message, error_code, title=None):
        self.message = message
        self.enum = error_code
        self.error_code = error_code

    def __str__(self):
        return self.message

    def get_error_response(self):
        return self.get_api_error_response()

    def get_api_error_response(self):
        return {
            api_constants.API_ERROR_CODE: self.error_code,
            api_constants.API_ERROR_MESSAGE: self.message,
        }

    def get_api_error_response_with_title(self, title=None):
        if title:
            self.errorTitle = title
        return {
            api_constants.API_ERROR_CODE: self.error_code,
            api_constants.API_ERROR_MESSAGE: self.message,
        }


class RaceConditionIntegrityError(CommonBaseException):
    message = 'Something went wrong'
    error_code = 'RaceCondition'

    def __init__(self):
        CommonBaseException.__init__(self, self.message, self.error_code)


class DataInconsistencyError(CommonBaseException):
    message = 'Data is inconsistent'
    error_code = 'dataInconsistency'

    def __init__(self, error_message=None):
        if error_message:
            self.message = error_message
        CommonBaseException.__init__(self, self.message, self.error_code)


class NotAnIntergerError(CommonBaseException):
    message = 'Not a number'
    error_code = 'notAnInterger'

    def __init__(self):
        CommonBaseException.__init__(self, self.message, self.error_code)


class UserNotPresentError(CommonBaseException):
    message = 'User id not passed'
    error_code = 'userNotPresent'

    def __init__(self):
        CommonBaseException.__init__(self, self.message, self.error_code)


class SyncDateTimeNotPresentError(CommonBaseException):
    message = 'please add a valid sync datetime(UTC) in your request'
    error_code = 'syncDateTimeNotPresentError'

    def __init__(self):
        CommonBaseException.__init__(self, self.message, self.error_code)


class InvalidDateTimeRangeError(CommonBaseException):
    message = 'the given date range is invalid'
    error_code = 'invalidDateTimeRange'

    def __init__(self):
        CommonBaseException.__init__(self, self.message, self.error_code)


class UserDoesNotExistError(CommonBaseException):
    message = 'User Does Not Exist in db'
    error_code = 'userDoesNotExist'

    def __init__(self):
        CommonBaseException.__init__(self, self.message, self.error_code)
