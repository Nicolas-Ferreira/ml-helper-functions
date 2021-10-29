# Keyboard - Matrix mapping
key_map = {
    '1': [0,0],
    '2': [0,1],
    '3': [0,2],
    '4': [0,3],
    '5': [0,4],
    '6': [0,5],
    '7': [0,6],
    '8': [0,7],
    '9': [0,8],
    '0': [0,9],
    '_': [0,10],
    'q': [1,0],
    'w': [1,1],
    'e': [1,2],
    'r': [1,3],
    't': [1,4],
    'y': [1,5],
    'u': [1,6],
    'i': [1,7],
    'o': [1,8],
    'p': [1,9],
    'a': [2,0],
    's': [2,1],
    'd': [2,2],
    'f': [2,3],
    'g': [2,4],
    'h': [2,5],
    'j': [2,6],
    'k': [2,7],
    'l': [2,8],
    'z': [3,0],
    'x': [3,1],
    'c': [3,2],
    'v': [3,3],
    'b': [3,4],
    'n': [3,5],
    'm': [3,6],
    '.': [3,8]
}


# Function to calculate Manhattan distance
def manhattan(a, b):
    return sum(abs(val1-val2) for val1, val2 in zip(a,b))


# Function to caclulate Average from list
def average(lst):
    return sum(lst) / len(lst)


# Function to calculate Keyboard Dispersion on String
def get_keyboard_dispersion(string):

    """
    Function to calculate character dispersion from a String

    :param string: string to calculate dispersion ratio
    :return: dispersion ratio, average distance from characters in string
    """
   
    list_chars = list(string)
   
    # Remove untracked keys
    keys = list(key_map.keys())
    list_chars = [x for x in list_chars if x in keys]
       
    list_distances = []

    for i in range(len(list_chars)-1):
        key1 = list_chars[i]
        key2 = list_chars[i+1]
        list_distances.append(manhattan(key_map.get(key1), key_map.get(key2)))
   
    return round(average(list_distances), 5)


string = 'dfsdfwewre'
print(f"Dispersion Score: {get_keyboard_dispersion(string)}")
