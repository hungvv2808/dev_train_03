from datetime import datetime

print(datetime.fromisoformat(str(datetime.now())).strftime('Order &ndash; %B %d, %Y @ %0l:%M %p'))