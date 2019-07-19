from datetime import datetime
date = '201958'

converted = datetime.strptime(date[:6], '%Y%m%d').date()

print(converted)