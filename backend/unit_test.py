#test for strings
def str_check(str_argument):
    if(isinstance(str_argument, str)):
        return 200
    else:
        return 406

#test for intigers
def int_check(int_argument):
    if(isinstance(int_argument, int)):
        return 200
    else:
        return 406

#test for bool values
def bool_check(bool_argument):
    if(isinstance(bool_argument, bool)):
        return 200
    else:
        return 406

#test for set abbreviation
def set_check(set_abbreviation):
    http_code = str_check(set_abbreviation)
    if(http_code == 200):
        if(len(set_abbreviation) == 3):
            return 200
        else:
            return 406
    else:
        return 406

