from datetime import datetime

now = datetime.fromisoformat('2007-05-18 11:46:26')

r = now.strftime("%B %d, %Y @ %0l:%M %p")
print("Test: ", r)