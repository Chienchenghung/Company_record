import get_data
import write_Gsheet
from googleapiclient.discovery import build

# get data from sql
con = get_data.get_data('USR_TBD', '20230420_google offline data.sql')
data = con.get_data_from_sql()

# write data to google sheet
num=len(data)+1+3
write_Gsheet.clean_sheet("[google sheet ID]",f'A4:F50000',{})
write_Gsheet.update_values("[google sheet ID]",
                f'A4:F{num}', "USER_ENTERED",
                data.values.tolist())
