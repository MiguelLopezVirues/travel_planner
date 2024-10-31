from scipy import stats
import numpy as np

def error_band(mean, std_dev, n, confidence=0.95):

    z_score = stats.norm.ppf((1 + confidence) / 2)
    

    standard_error = std_dev / np.sqrt(n)
    
    margin_of_error = z_score * standard_error

    lower_bound = mean - margin_of_error
    upper_bound = mean + margin_of_error
    return lower_bound, upper_bound, margin_of_error

lower, upper, margin_of_error = error_band(8.1, 3, 9000, 0.95)