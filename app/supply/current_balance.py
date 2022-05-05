import sql_functions
import os

from worker import Worker
from adm.admin import get_data_db
from adm.redash import get_result
from config import API_KEY


class CurrentBalance(Worker):
    def __init__(self, sender, channel_id):
        super().__init__(sender, channel_id)

    def current_balance(self, user_id):
        data = get_result(139, {}, API_KEY)
        data = data.reindex(sorted(data.columns), axis=1)
        data.to_excel('filename.xlsx', index=False)
        file = 'filename.xlsx'
        self.sender.send_files(file, user_id, file, file)
        os.remove(file)
        return '', 200
