from datetime import datetime
date = '201958^i'

converted = datetime.strptime(date[:6], '%Y%m%d')

print(converted)