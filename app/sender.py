import slack


class Sender:
    def __init__(self, bot_token):
        self.client = slack.WebClient(token=bot_token)

    def send_message(self, channel, text):
        message = self.client.chat_postMessage(channel=channel, text=text)
        correct = message["ok"]
        if correct:
            pass
        else:
            self.client.chat_postMessage(channel=channel, text=text)

    def send_files(self, files, channels, filename, title):
        upload = self.client.api_call(api_method="files.upload", files={
            'file': files,
        }, data={
            'channels': channels,
            'filename': filename,
            'title': title
        })
        correct = upload["ok"]
        if correct:
            pass
        else:
            self.client.api_call(api_method="files.upload", files={
                'file': files,
            }, data={
                'channels': channels,
                'filename': filename,
                'title': title
            })
