import numpy as np
import re

def convert_to_hours(time_str):
    time_str = str(time_str).strip()

    if re.match(r'^\d+$', time_str):  # If just a single number
        return int(time_str)

    if re.search(r'(\d+.*h|\d+.*m|horas|minutos|d|días|dias)', time_str):

        time_str = re.sub(r'(\d+)\s*horas', r'\1*60', time_str)
        time_str = re.sub(r'(\d+)\s*h', r'\1*60', time_str)

        time_str = re.sub(r'(\d+)\s*minutos', r'+\1', time_str)
        time_str = re.sub(r'(\d+)\s*m', r'+\1', time_str)

        time_str = re.sub(r'(\d+)\s*días', r'+\1*60*24', time_str)
        time_str = re.sub(r'(\d+)\s*dias', r'+\1*60*24', time_str)
        time_str = re.sub(r'(\d+)\s*d', r'+\1*60*24', time_str)
        
        time_str = re.sub(r'[^\d*+]', '', time_str)
        
        return eval(time_str)/60 if time_str else np.nan
    return np.nan