from datetime import datetime, timedelta
import re

def parse_time_ago(time_ago_str):
    # Get the current time
    now = datetime.now()
    
    # Regular expression to match the time ago format
    pattern = r"(\d+)\s+(secs?|mins?|hrs?|days?)\s+ago"
    match = re.match(pattern, time_ago_str)
    
    if not match:
        return None
    
    # Extract the quantity and unit from the string
    quantity = int(match.group(1))
    unit = match.group(2)
    
    # Calculate the timedelta based on the unit
    if unit.startswith('sec'):
        delta = timedelta(seconds=quantity)
    elif unit.startswith('min'):
        delta = timedelta(minutes=quantity)
    elif unit.startswith('hr'):
        delta = timedelta(hours=quantity)
    elif unit.startswith('day'):
        delta = timedelta(days=quantity)
    else:
        raise ValueError(f"Unknown time unit: {unit}")
    
    # Subtract the delta from the current time to get the target time
    target_time = now - delta
    return target_time

# Example usage
time_strings = ["13 hrs ago", "22 mins ago", "2 days ago"]
#for time_str in time_strings:
#    print(f"{time_str} -> {parse_time_ago(time_str)}")
#print(parse_time_ago(""))
