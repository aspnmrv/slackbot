import datetime
import random

from jira import JIRA
from worker import Worker
from config import JIRA_BASIC_AUTH, IDS_USERS_SLACK
from globals import list_holiday


class Jira(Worker):

    def __init__(self, sender, channel_id):
        super().__init__(sender, channel_id)


    @staticmethod
    def get_reminder_text(issues_list, ids) -> str:
        """"""
        assignees = set([issue.fields.assignee.displayName for issue in issues_list if issue is not None])
        message_text = "🚒 Сегодня Critical дедлайн у следующих задач: \n\n"
        for assignee in assignees:
            if assignee in ids.keys():
                message_text += "<@" + ids[assignee] + ">" + ": \n \n"
            else:
                message_text += assignee + " : \n \n"

            for issue in issues_list:
                if issue.fields.assignee.displayName == assignee:
                    message_text += "❗️ Задача: " + issue.fields.summary + "\n" + "Ссылка: " \
                                    f"https://peoplelabs.atlassian.net/browse/{issue.key} \n" + "Дедлайн: " \
                                    + issue.fields.duedate + "\n \n"
        message_text += "Просто напомнил.. :troll2keke:"

        return message_text

    @staticmethod
    def get_overdue_text(issues_list, ids) -> str:
        """"""
        assignees = set([issue.fields.assignee.displayName for issue in issues_list if issue is not None])
        message_text = "⚡️ Следующие Задачи просрочены: \n \n"
        for assignee in assignees:
            if assignee in ids.keys():
                message_text += "<@" + ids[assignee] + ">" + ": \n \n"
            else:
                message_text += assignee + " : \n \n"

            for issue in issues_list:
                if issue.fields.assignee.displayName == assignee:
                    message_text += "‼️️ Задача: " + issue.fields.summary + "\n" + "Ссылка: " \
                                    f"https://peoplelabs.atlassian.net/browse/{issue.key} \n" + "Дедлайн был: " \
                                            + issue.fields.duedate + "\n \n"

        return message_text

    def send(self):
        """ Select tasks from Jira with cond and send in channel attention"""

        current_date = datetime.datetime.today()
        hello_text = random.choice(list_holiday) + " 🎄 "
        if (current_date.month == 12 and current_date.day == 31) or \
            (current_date.month == 1 and 1 <= current_date.day <= 10):
            self.sender.send_message(self.channel_id, hello_text)
        else:
            ids = IDS_USERS_SLACK
            jira = JIRA("https://peoplelabs.atlassian.net/", basic_auth=JIRA_BASIC_AUTH)
            issues_list = jira.search_issues("project = DA and duedate = startOfDay(0d) "
                                             "and status in ('TO DO', 'IN PROGRESS') and assignee is not null")
            issues_overdue = jira.search_issues("project = DA and duedate <= startOfDay(-1d) "
                                                "and status in ('TO DO', 'IN PROGRESS') and assignee is not null")
            if len(issues_overdue) == 0:
                if len(issues_list) == 0:
                    self.sender.send_message(self.channel_id, "Просроченных задачек нет! :party-parrot:")
                else:
                    self.sender.send_message(self.channel_id, "Просроченных задачек нет! Но ... ")

                    self.sender.send_message(self.channel_id, self.get_reminder_text(issues_list, ids))
            else:
                self.sender.send_message(self.channel_id, self.get_overdue_text(issues_overdue, ids))

                if len(issues_list) == 0:
                    pass
                else:
                    self.sender.send_message(self.channel_id, self.get_reminder_text(issues_list, ids))
