acc_block = {
    "text": "Thanks for your request, we'll process it and get back to you."
}

test_block = {
    "text": "This is a test slash command. It works and you are amazing!"
}


def get_two_datepickers_block(date_start, date_end, value):
    return {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": "Выберите даты:",
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "datepicker",
                        "initial_date": date_start,#date_dict['date_week_prev'],
                        "placeholder": {
                            "type": "plain_text",
                            "text": "Select a date",
                        },
                        "action_id": value + '_start'
                    },
                    {
                        "type": "datepicker",
                        "initial_date": date_end,#date_dict['date_now'],
                        "placeholder": {
                            "type": "plain_text",
                            "text": "Select a date",
                        },
                        "action_id": value + '_end'
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Подтвердить"
                        },
                        "value": value,
                        "action_id": value + '_confirm'
                    }
                ]
            }
        ]
    }
